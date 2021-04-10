# Raft implementation
What is the better way to understand Raft Consensus Algorithm than implementing it from scratch.

# TODO
- test ApplyCommittedEntries
- test AddCommand to 100% coverage
- testing follower AppendEntry w/ 100% coverage
- testing follower RequestVoteFollower w/ 100% coverage
- testing leader w/ 100% coverage
- refactor states directly to RaftNode
- Snapshots, using the Chandy-Lamport or another algorithm
- Membership Changes (§6 of the Raft paper and §4 of the dissertation)
- Log Compaction (§7 of the Raft paper and §5 of the dissertation)
- Web application to control and interface with a Raft node (using gRPC)
- compare to hashicorp & etcd implementation 
- compare my implementationt to this blog post: https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/


## raft implementations in production
- https://github.com/hashicorp/raft
- https://github.com/etcd-io/etcd/blob/master/raft/README.md


## Sources
- https://github.com/etcd-io/etcd/blob/master/raft/README.md
- https://ieftimov.com/post/understanding-bytes-golang-build-tcp-protocol/
- https://opensource.com/article/18/5/building-concurrent-tcp-server-go

- https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
- https://rcrowley.org/articles/golang-graceful-stop.html , https://gist.github.com/rcrowley/5474430
- ? https://www.rodrigoaraujo.me/posts/golang-pattern-graceful-shutdown-of-concurrent-events/
- ? https://github.com/stephenafamo/orchestra
