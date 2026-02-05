//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cluster Membership open source project
//
// Copyright (c) 2018-2022 Apple Inc. and the Swift Cluster Membership project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.md for the list of Swift Cluster Membership project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import ClusterMembership
import Logging
import NIO
import NIOCore
import SWIM
import SWIMTestKit
import Synchronization
import XCTest

import struct Foundation.Date
import class Foundation.NSLock

@testable import SWIMNIOExample

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Real Networking Test Case

struct RealCluster {
    let session: ClusterSession
    let group: MultiThreadedEventLoopGroup
    let loop: EventLoop

    init(
        session: ClusterSession,
        group: MultiThreadedEventLoopGroup,
        loop: EventLoop
    ) {
        self.session = session
        self.group = group
        self.loop = loop
    }

    func makeClusterNode(
        name: String? = nil,
        configure configureSettings: (inout SWIMNIO.Settings) -> Void = { _ in () }
    ) async throws -> (SWIMNIOHandler, Channel) {
        let port = self.session.nextPort()
        let name = name ?? "swim-\(port)"
        var settings = SWIMNIO.Settings()
        configureSettings(&settings)

        if self.session.captureLogs {
            self.session.makeLogCapture(name: name, settings: &settings)
        }

        let handler = SWIMNIOHandler(settings: settings)
        let bootstrap = DatagramBootstrap(group: self.group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in channel.pipeline.addHandler(handler) }

        let channel = try await bootstrap.bind(host: "127.0.0.1", port: port).get()

        self.session._shells.withLock { $0.append(handler.shell) }
        self.session._nodes.withLock { $0.append(handler.shell.node) }

        return (handler, channel)
    }

    public func capturedLogs(of node: Node) -> LogCapture {
        self.session.capturedLogs(of: node)
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Embedded Networking Test Case
struct EmbeddedCluster {
    let session: ClusterSession
    let loop: EmbeddedEventLoop

    init(session: ClusterSession, loop: EmbeddedEventLoop) {
        self.session = session
        self.loop = loop
    }

    func makeEmbeddedShell(
        _ _name: String? = nil,
        configure: (inout SWIMNIO.Settings) -> Void = { _ in () }
    ) -> SWIMNIOShell {
        var settings = SWIMNIO.Settings()
        configure(&settings)
        let node: Node
        if let _node = settings.swim.node {
            node = _node
        } else {
            let port = self.session.nextPort()
            let name = _name ?? "swim-\(port)"
            node = Node(
                protocol: "test",
                name: name,
                host: "127.0.0.1",
                port: port,
                uid: .random(in: 1..<UInt64.max)
            )
        }

        if self.session.captureLogs {
            self.session.makeLogCapture(name: node.name ?? "swim-\(node.port)", settings: &settings)
        }

        let channel = EmbeddedChannel(loop: self.loop)
        channel.isWritable = true
        let shell = SWIMNIOShell(
            node: node,
            settings: settings,
            channel: channel,
            onMemberStatusChange: { _ in () }  // TODO: store events so we can inspect them?
        )

        self.session._nodes.withLock { $0.append(shell.node) }
        self.session._shells.withLock { $0.append(shell) }

        return shell
    }

    public func capturedLogs(of node: Node) -> LogCapture {
        self.session.capturedLogs(of: node)
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Base

final class ClusterSession: Sendable {
    static let shared = ClusterSession()

    public let _nodes: Mutex<[Node]> = Mutex([])
    public let _shells: Mutex<[SWIMNIOShell]> = Mutex([])
    public let _logCaptures: Mutex<[LogCapture]> = Mutex([])

    /// If `true` automatically captures all logs of all `setUpNode` started systems, and prints them if at least one test failure is encountered.
    /// If `false`, log capture is disabled and the systems will log messages normally.
    ///
    /// - Default: `true`
    var captureLogs: Bool {
        true
    }

    /// Enables logging all captured logs, even if the test passed successfully.
    /// - Default: `false`
    var alwaysPrintCaptureLogs: Bool {
        false
    }

    let _nextPort = Mutex(9001)
    func nextPort() -> Int {
        defer { self._nextPort.withLock { $0 += 1 } }
        return self._nextPort.withLock { $0 }
    }

    func configureLogCapture(settings: inout LogCapture.Settings) {
        // just use defaults
    }

    func closeAllShellChannels() async {
        for shell in self._shells.withLock({ $0 }) {
            do {
                try await shell.myself.channel.close()
            } catch {
                // ok: already closed
            }
        }
    }

    deinit {
        if self.captureLogs, self.alwaysPrintCaptureLogs {
            self.printAllCapturedLogs()
        }

        self._nodes.withLock { $0 = [] }
        self._logCaptures.withLock { $0 = [] }
    }

    //  open override func tearDown() {
    //
    //    let testsFailed = self.testRun?.totalFailureCount ?? 0 > 0

    //

    //  }

    func makeLogCapture(name: String, settings: inout SWIMNIO.Settings) {
        var captureSettings = LogCapture.Settings()
        self.configureLogCapture(settings: &captureSettings)
        let capture = LogCapture(settings: captureSettings)

        settings.logger = capture.logger(label: name)

        self._logCaptures.withLock { $0.append(capture) }
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Captured Logs

extension ClusterSession {
    public func capturedLogs(of node: Node) -> LogCapture {
        guard let index = self._nodes.withLock({ $0.firstIndex(of: node) }) else {
            fatalError("No such node: [\(node)] in [\(self._nodes.withLock { $0 })]!")
        }

        return self._logCaptures.withLock { $0[index] }
    }

    public func printCapturedLogs(of node: Node) {
        print(
            "------------------------------------- \(node) ------------------------------------------------"
        )
        self.capturedLogs(of: node).printLogs()
        print(
            "========================================================================================================================"
        )
    }

    public func printAllCapturedLogs() {
        for node in self._nodes.withLock({ $0 }) {
            self.printCapturedLogs(of: node)
        }
    }
}

func withRealCluster<T>(
    session: ClusterSession,
    group: MultiThreadedEventLoopGroup,
    _ body: (RealCluster) async throws -> T
) async throws -> T {
    let cluster = RealCluster(session: session, group: group, loop: group.next())
    do {
        let result = try await body(cluster)
        await session.closeAllShellChannels()
        try await group.shutdownGracefully()

        return result
    } catch {

        await session.closeAllShellChannels()
        try await group.shutdownGracefully()
        throw error
    }
}

func withEmbeddedCluster<T>(
    session: ClusterSession,
    _ body: (EmbeddedCluster) async throws -> T
) async throws -> T {
    let loop = EmbeddedEventLoop()
    defer { try? loop.close() }

    let cluster = EmbeddedCluster(session: session, loop: loop)

    do {
        let result = try await body(cluster)
        await session.closeAllShellChannels()
        return result
    } catch {
        await session.closeAllShellChannels()
        throw error
    }
}
