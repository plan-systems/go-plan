module github.com/plan-systems/plan-core

go 1.12

require (
	github.com/dgraph-io/badger v1.6.0
	github.com/golang/protobuf v1.3.2
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/errors v0.8.1
	github.com/plan-systems/klog v0.0.0-20190618231738-14c6677fa6ea
	github.com/plan-systems/plan-pdi-local v0.0.0-20190713092621-7d241122cbd5
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4
	google.golang.org/grpc v1.22.0
)

// replace github.com/plan-systems/plan-pdi-local => ../plan-pdi-local
