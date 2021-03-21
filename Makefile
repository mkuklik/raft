export PATH := $(shell go env GOPATH)/bin:${PATH}

grpc: 
	protoc -I=./raftpb --go_out=./raftpb --go_opt=paths=source_relative \
		--go-grpc_out=./raftpb --go-grpc_opt=paths=source_relative \
		./raftpb/raft.proto