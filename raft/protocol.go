package raft

// https://ieftimov.com/post/understanding-bytes-golang-build-tcp-protocol/
// https://blog.golang.org/gob

// when joining follower, follower will reply with message RedirectMessage, which
