# Shiva: Background

Shiva is a distributed database which relies on the RAFT consensus algorithm to guarantee safe reads and rights to it stores and tables. We wish to test for its behavior under failure and wish to ensure that it is proof from most standard error such as node failures or loss of data.

# Shiva Jepsen Test

A Jepsen test written in Clojure to test the guarantees of the Shiva Distributed Database (property of Transwarp). 

# Usage

This is used to confirm that the RAFT groups for Shiva Distributed Database upholds its guarantees. Faults are injected into the base, such as simulated crashes and disconnects, to gage how well the RAFT groups function under failure. 

# About

This test has been used to uncover several mistakes within Transwarp's implementation of RAFT, particularly mismatching pieces of data in RPCs (messages that is sent in between RAFT nodes and clients for a variety of purposes which include message updates as well as rebalances and node migration). This test has also uncovered weaknesses in Shiva's design which needs to be fixed, particularly its read operations which when the RAFT group is in unstable condition, would return inconsistent results, breaking the consistency guarantee.
