#!/bin/bash


# if [[ -d "../../plan-protobufs" ]]; then
#     echo "=====  updating plan-protobufs  ====="
#     git -C ../../plan-protobufs pull
#     $cmd
# fi

SELF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# go get -u github.com/gogo/protobuf/protoc-gen-gofast
# go get -u google.golang.org/grpc

BUILD_GO_PROTO="$SELF_DIR/../../plan-protobufs/build-proto.sh"
DST_DIR="$SELF_DIR/.."

$BUILD_GO_PROTO ski   gofast "$DST_DIR"
$BUILD_GO_PROTO repo  gofast "$DST_DIR"
$BUILD_GO_PROTO vault gofast "$DST_DIR"

# echo "=====  generating protobufs  ====="
# go get -u github.com/golang/protobuf/protoc-gen-go
# go generate .

echo "=====  building pnoode  ====="
go build --tags static -o pnode



