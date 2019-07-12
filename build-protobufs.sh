#!/bin/bash
set -e

help() {
    cat <<EOF
build-protobufs [--protos \$PROTO_INCLUDE] [--dest \$DEST_DIR]

Builds the protobuf outputs for plan-core using this operating system's
protoc toolchain and the gofast plugin, both of which should be installed
in your \$PATH.

--protos (or \$PROTO_INCLUDE) path to directoy containing the top-level
         directory of .proto files. Defaults to looking in plan-protobufs
         source's 'pkg/' directory if it's installed in the \$GOPATH.

--dest   (or \$DEST_DIR) path to directory where '.pb.go' files will be
         written. Defaults to top-level source directory of the plan-core
         source if it's installed in the \$GOPATH.
EOF
    exit 0
}

err_exit() {
    echo "$1"
    exit 1
}

src="${GOPATH}/src/github.com/plan-systems"
PROTO_INCLUDE="${PROTO_INCLUDE:-${src}/plan-protobufs/pkg}"
DEST_DIR="${DEST_DIR:-${src}/plan-core}"

check() {
    command -v "protoc" > /dev/null || err_exit "protoc not on \$PATH ($PATH)"
    command -v "protoc-gen-gofast"  > /dev/null \
        || err_exit "gofast plugin not on \$PATH ($PATH)"
    if [ ! -d "$PROTO_INCLUDE" ]; then
        err_exit "--protos not set to a valid directory: $PROTO_INCLUDE"
    fi
    if [ ! -d "$DEST_DIR" ]; then
        err_exit "--dest not set to a valid directory: $DEST_DIR"
    fi
}

compile() {
    pkg=$1
    proto_file_path="${PROTO_INCLUDE}/${pkg}/${pkg}.proto"

    protoc -I="$PROTO_INCLUDE" \
           --gofast_out=plugins="grpc:${DEST_DIR}" \
           "$proto_file_path"

    # we can't use the go_package option in our .proto file because
    # we want to potentially reuse them across packages. so we end
    # up having to edit the generated file to match our package here.
    replace='s~#PKG# "#PKG#"~#PKG# "github.com/plan-systems/plan-core/#PKG#" /// Redirected by build-go-protobufs :)~'
    output_file="$DEST_DIR/$pkg/$pkg.pb.go"
    sed \
        -e "${replace//#PKG#/plan}" 	\
        -e "${replace//#PKG#/ski}"		\
        -e "${replace//#PKG#/client}"   \
        -e "${replace//#PKG#/pdi}"		\
        -e "${replace//#PKG#/repo}" 	\
        "$output_file" > "${output_file}.tmp"
    mv "${output_file}.tmp" "$output_file"
}

# accept command line arguments to override environment
# variables or default values
while [[ $# -gt 0 ]]
do
    arg="$1"
    val="${2:-}"
    case "$arg" in
        --protos) PROTO_INCLUDE="$val" ;;
        --dest) DEST_DIR="$val" ;;
        --help) help ;;
        *) err_exit "invalid argument" ;;
    esac
    shift
    shift
done

check
compile client
compile pdi
compile plan
compile repo
compile ski
