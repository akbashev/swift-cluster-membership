//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cluster Membership open source project
//
// Copyright (c) 2018-2019 Apple Inc. and the Swift Cluster Membership project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.md for the list of Swift Cluster Membership project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import ClusterMembership
import NIO
import SWIM
@testable import SWIMNIOExample
import SWIMTestKit
import XCTest
import Synchronization

// TODO: those tests could be done on embedded event loops probably
final class SWIMNIOEventClusteredTests: EmbeddedClusteredXCTestCase {
    var settings: SWIMNIO.Settings = SWIMNIO.Settings(swim: .init())
    lazy var myselfNode = Node(protocol: "udp", host: "127.0.0.1", port: 7001, uid: 1111)
    lazy var myselfPeer = SWIM.NIOPeer(node: myselfNode, channel: EmbeddedChannel())
    lazy var myselfMemberAliveInitial = SWIM.Member(peer: myselfPeer, status: .alive(incarnation: 0), protocolPeriod: 0)

    var group: MultiThreadedEventLoopGroup!

    override func setUp() {
        super.setUp()

        self.settings.node = self.myselfNode

        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDown() {
        try! self.group.syncShutdownGracefully()
        self.group = nil
        super.tearDown()
    }

    func test_memberStatusChange_alive_emittedForMyself() async throws {
        let firstProbe = ProbeEventHandler(loop: group.next())

        let first = try await bindShell(probe: firstProbe) { settings in
            settings.node = self.myselfNode
        }

        try firstProbe.expectEvent(SWIM.MemberStatusChangedEvent(previousStatus: nil, member: self.myselfMemberAliveInitial))
        
        try await first.close().get()
    }

    func test_memberStatusChange_suspect_emittedForDyingNode() async throws {
        let firstProbe = ProbeEventHandler(loop: group.next())
        let secondProbe = ProbeEventHandler(loop: group.next())

        let secondNodePort = 7002
        let secondNode = Node(protocol: "udp", host: "127.0.0.1", port: secondNodePort, uid: 222_222)

        let second = try await bindShell(probe: secondProbe) { settings in
            settings.node = secondNode
        }

        let first = try await bindShell(probe: firstProbe) { settings in
            settings.node = self.myselfNode
            settings.swim.initialContactPoints = [secondNode.withoutUID]
        }

        // wait for second probe to become alive:
        try secondProbe.expectEvent(
            SWIM.MemberStatusChangedEvent(
                previousStatus: nil,
                member: SWIM.Member(peer: SWIM.NIOPeer(node: secondNode, channel: EmbeddedChannel()), status: .alive(incarnation: 0), protocolPeriod: 0)
            )
        )

        try await Task.sleep(for: .seconds(5)) // let them discover each other, since the nodes are slow at retrying and we didn't configure it yet a sleep is here meh
        try await second.close().get()

        try firstProbe.expectEvent(SWIM.MemberStatusChangedEvent(previousStatus: nil, member: self.myselfMemberAliveInitial))

        let secondAliveEvent = try firstProbe.expectEvent()
        XCTAssertTrue(secondAliveEvent.isReachabilityChange)
        XCTAssertTrue(secondAliveEvent.status.isAlive)
        XCTAssertEqual(secondAliveEvent.member.node.withoutUID, secondNode.withoutUID)

        let secondDeadEvent = try firstProbe.expectEvent()
        XCTAssertTrue(secondDeadEvent.isReachabilityChange)
        XCTAssertTrue(secondDeadEvent.status.isDead)
        XCTAssertEqual(secondDeadEvent.member.node.withoutUID, secondNode.withoutUID)
        
        try await first.close().get()
    }

    private func bindShell(
        probe probeHandler: ProbeEventHandler,
        configure: (inout SWIMNIO.Settings) -> Void = { _ in () }
    ) async throws -> Channel {
        var settings = self.settings
        configure(&settings)
        self.makeLogCapture(name: "swim-\(settings.node!.port)", settings: &settings)

        self._nodes.append(settings.node!)
        return try await DatagramBootstrap(group: self.group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { [settings] channel in
                let swimHandler = SWIMNIOHandler(settings: settings)
                return channel.pipeline.addHandler(swimHandler).flatMap { _ in
                    channel.pipeline.addHandler(probeHandler)
                }
            }.bind(host: settings.node!.host, port: settings.node!.port)
            .get()
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Test Utils

extension ProbeEventHandler {
    @discardableResult
    func expectEvent(
        _ expected: SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>? = nil,
        file: StaticString = (#file), line: UInt = #line
    ) throws -> SWIM.MemberStatusChangedEvent<SWIM.NIOPeer> {
        let got = try self.expectEvent()

        if let expected = expected {
            XCTAssertEqual(got, expected, file: file, line: line)
        }

        return got
    }
}

final class ProbeEventHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>

    let events: Mutex<[SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>]> = .init([])
    // FIXME: Move to Swift Concurrency
    let waitingPromise: Mutex<EventLoopPromise<SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>>?> = .init(.none)
    let loop: Mutex<EventLoop>

    init(loop: EventLoop) {
        self.loop = .init(loop)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let change = self.unwrapInboundIn(data)
        self.events.withLock { $0.append(change) }

        if let probePromise = self.waitingPromise.withLock({ $0 }) {
            let event = self.events.withLock { $0.removeFirst() }
            probePromise.succeed(event)
            self.waitingPromise.withLock { $0 = .none }
        }
    }

    func expectEvent(file: StaticString = #file, line: UInt = #line) throws -> SWIM.MemberStatusChangedEvent<SWIM.NIOPeer> {
        let p = self.loop.withLock { $0.makePromise(of: SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>.self, file: file, line: line) }
        return try self.loop.withLock {
            $0.execute {
                assert(self.waitingPromise.withLock { $0 == nil}, "Already waiting on an event")
                if !self.events.withLock({ $0.isEmpty }) {
                    let event = self.events.withLock { $0.removeFirst() }
                    p.succeed(event)
                } else {
                    self.waitingPromise.withLock { $0 = p }
                }
            }
            return try p.futureResult.wait()
        }
    }
}
