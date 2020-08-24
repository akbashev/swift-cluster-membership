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
import struct Dispatch.DispatchTime
import enum Dispatch.DispatchTimeInterval

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: MemberStatusChangeEvent

extension SWIM {
    /// Emitted whenever a membership change happens.
    public struct MemberStatusChangedEvent: CustomStringConvertible, Equatable {
        public let member: SWIM.Member

        /// The resulting ("current") status of the `member`.
        public var status: SWIM.Status {
            // Note if the member is marked .dead, SWIM shall continue to gossip about it for a while
            // such that other nodes gain this information directly, and do not have to wait until they detect
            // it as such independently.
            self.member.status
        }

        /// Previous status of the member, needed in order to decide if the change is "effective" or if applying the
        /// member did not move it in such way that we need to inform the cluster about unreachability.
        public let previousStatus: SWIM.Status?

        public init(previousStatus: SWIM.Status?, member: SWIM.Member) {
            if let from = previousStatus, from == .dead {
                precondition(member.status == .dead, "Change MUST NOT move status 'backwards' from [.dead] state to anything else, but did so, was: \(member)")
            }

            self.previousStatus = previousStatus
            self.member = member
        }

        /// Reachability changes are important events, in which a reachable node became unreachable, or vice-versa,
        /// as opposed to events which only move a member between `.alive` and `.suspect` status,
        /// during which the member should still be considered and no actions assuming it's death shall be performed (yet).
        ///
        /// If true, a system may want to issue a reachability change event and handle this situation by confirming the node `.dead`,
        /// and proceeding with its removal from the cluster.
        public var isReachabilityChange: Bool {
            guard let fromStatus = self.previousStatus else {
                // i.e. nil -> anything, is always an effective reachability affecting change
                return true
            }

            // explicitly list all changes which are affecting reachability, all others do not (i.e. flipping between
            // alive and suspect does NOT affect high-level reachability).
            switch (fromStatus, self.status) {
            case (.alive, .unreachable),
                 (.alive, .dead):
                return true
            case (.suspect, .unreachable),
                 (.suspect, .dead):
                return true
            case (.unreachable, .alive),
                 (.unreachable, .suspect):
                return true
            case (.dead, .alive),
                 (.dead, .suspect),
                 (.dead, .unreachable):
                fatalError("Change MUST NOT move status 'backwards' from .dead state to anything else, but did so, was: \(self)")
            default:
                return false
            }
        }

        public var description: String {
            var res = "SWIM.MemberStatusChangeEvent(\(self.member), previousStatus: "
            if let previousStatus = self.previousStatus {
                res += "\(previousStatus)"
            } else {
                res += "<unknown>"
            }
            res += ")"
            return res
        }
    }
}