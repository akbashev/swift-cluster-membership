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
import SWIMTestKit
import Testing

@testable import SWIMNIOExample

// TODO: those tests could be done on embedded event loops probably
@Suite(.serialized)
class SWIMNIOEventClusteredTests {

    let session = ClusterSession()
    var settings: SWIMNIO.Settings = SWIMNIO.Settings(swim: .init())
    lazy var myselfNode = Node(protocol: "udp", host: "127.0.0.1", port: 7001, uid: 1111)
    lazy var myselfPeer = SWIM.NIOPeer(node: myselfNode, channel: EmbeddedChannel())
    lazy var myselfMemberAliveInitial = SWIM.Member(
        peer: myselfPeer,
        status: .alive(incarnation: 0),
        protocolPeriod: 0
    )

    var group: MultiThreadedEventLoopGroup!

    init() {
        self.settings.node = self.myselfNode
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    deinit {
        try! self.group.syncShutdownGracefully()
        self.group = nil
    }

    @Test
    func test_memberStatusChange_alive_emittedForMyself() async throws {
        let firstProbe = ProbeEventHandler(loop: group.next())

        let first = try bindShell(probe: firstProbe) { settings in
            settings.node = self.myselfNode
        }

        do {
            try await firstProbe.expectEvent(
                SWIM.MemberStatusChangedEvent(previousStatus: nil, member: self.myselfMemberAliveInitial)
            )
            try await first.close().get()
        } catch {
            try await first.close().get()
            throw error
        }
    }

    @Test
    func test_memberStatusChange_suspect_emittedForDyingNode() async throws {
        let firstProbe = ProbeEventHandler(loop: group.next())
        let secondProbe = ProbeEventHandler(loop: group.next())

        let secondNodePort = 7002
        let secondNode = Node(protocol: "udp", host: "127.0.0.1", port: secondNodePort, uid: 222_222)

        let second = try bindShell(probe: secondProbe) { settings in
            settings.node = secondNode
        }

        let first = try bindShell(probe: firstProbe) { settings in
            settings.node = self.myselfNode
            settings.swim.initialContactPoints = [secondNode.withoutUID]
        }
        defer { Task { try await first.close().get() } }

        // wait for second probe to become alive:
        try await secondProbe.expectEvent(
            SWIM.MemberStatusChangedEvent(
                previousStatus: nil,
                member: SWIM.Member(
                    peer: SWIM.NIOPeer(node: secondNode, channel: EmbeddedChannel()),
                    status: .alive(incarnation: 0),
                    protocolPeriod: 0
                )
            )
        )

        try await Task.sleep(for: .seconds(5))  // let them discover each other, since the nodes are slow at retrying and we didn't configure it yet a sleep is here meh
        try! await second.close().get()

        try await firstProbe.expectEvent(
            SWIM.MemberStatusChangedEvent(previousStatus: nil, member: self.myselfMemberAliveInitial)
        )

        let secondAliveEvent = try await firstProbe.expectEvent()
        #expect(secondAliveEvent.isReachabilityChange)
        #expect(secondAliveEvent.status.isAlive)
        #expect(secondAliveEvent.member.node.withoutUID == secondNode.withoutUID)

        let secondDeadEvent = try await firstProbe.expectEvent()
        #expect(secondDeadEvent.isReachabilityChange)
        #expect(secondDeadEvent.status.isDead)
        #expect(secondDeadEvent.member.node.withoutUID == secondNode.withoutUID)
    }

    private func bindShell(
        probe probeHandler: ProbeEventHandler,
        configure: (inout SWIMNIO.Settings) -> Void = { _ in () }
    ) throws -> Channel {
        var settings = self.settings
        configure(&settings)
        self.session.makeLogCapture(name: "swim-\(settings.node!.port)", settings: &settings)

        self.session._nodes.withLock({ $0.append(settings.node!) })
        return try DatagramBootstrap(group: self.group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in

                let swimHandler = SWIMNIOHandler(settings: settings)
                return channel.pipeline.addHandler(swimHandler).flatMap { _ in
                    channel.pipeline.addHandler(probeHandler)
                }
            }.bind(host: settings.node!.host, port: settings.node!.port).wait()
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Test Utils

extension ProbeEventHandler {
    @discardableResult
    func expectEvent(
        _ expected: SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>? = nil,
        file: StaticString = (#file),
        line: UInt = #line,
        sourceLocation: SourceLocation = #_sourceLocation
    ) async throws -> SWIM.MemberStatusChangedEvent<SWIM.NIOPeer> {
        let got = try await self.expectEvent()

        if let expected = expected {
            #expect(got == expected, sourceLocation: sourceLocation)
        }

        return got
    }
}

final class ProbeEventHandler: ChannelInboundHandler {
    typealias InboundIn = SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>

    var events: [SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>] = []
    var waitingPromise: EventLoopPromise<SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>>?
    var loop: EventLoop

    init(loop: EventLoop) {
        self.loop = loop
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let change = self.unwrapInboundIn(data)
        self.events.append(change)

        if let probePromise = self.waitingPromise {
            let event = self.events.removeFirst()
            probePromise.succeed(event)
            self.waitingPromise = nil
        }
    }

    func expectEvent(
        file: StaticString = #file,
        line: UInt = #line
    ) async throws
        -> SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>
    {
        let p = self.loop.makePromise(
            of: SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>.self,
            file: file,
            line: line
        )
        self.loop.execute {
            assert(self.waitingPromise == nil, "Already waiting on an event")
            if !self.events.isEmpty {
                let event = self.events.removeFirst()
                p.succeed(event)
            } else {
                self.waitingPromise = p
            }
        }
        return try await p.futureResult.get()
    }
}
