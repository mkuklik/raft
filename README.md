# raft
Raft implementation

# TODO
- testing follower AppendEntry w/ 100% coverage
- testing follower RequestVoteFollower w/ 100% coverage
- load logs, persist log on append
- persist log and persistant state when required
- refactor states directly to RaftNode
- snapshot
- tests ???
- Membership Changes (§6 of the Raft paper and §4 of the dissertation)
- Log Compaction (§7 of the Raft paper and §5 of the dissertation)
- Web application to control and interface with a Raft node (using gRPC)
- Snapshots, using the Chandy-Lamport or another algorithm


Sources:
- https://github.com/etcd-io/etcd/blob/master/raft/README.md
- https://ieftimov.com/post/understanding-bytes-golang-build-tcp-protocol/
- https://opensource.com/article/18/5/building-concurrent-tcp-server-go

- https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
- https://rcrowley.org/articles/golang-graceful-stop.html , https://gist.github.com/rcrowley/5474430
- ? https://www.rodrigoaraujo.me/posts/golang-pattern-graceful-shutdown-of-concurrent-events/
- ? https://github.com/stephenafamo/orchestra
