#!/bin/bash
#
# See:
#    plan-systems/plan-protobuf/README.md
#    http://plan-systems.org
#
#
set -e

SELF=$(basename "$0")

if [[ $# -ne 2 ]]; then
    echo "Usage: ./build-go-proto.sh <plan_pkg_name> <out_path>"
    exit
fi

PKG_NAME="$1"
DST_DIR="$2"

BUILD_PROTO="../plan-protobufs/build-proto.sh"
$BUILD_PROTO "$PKG_NAME" gofast "$DST_DIR"

# we need the canonical go import path, so edit the generated file
replace='s~#PKG# "#PKG#"~#PKG# "github.com/plan-systems/plan-core" /// Redirected by '$SELF' :)~'

echo "$DST_DIR/$PKG_NAME/$PKG_NAME.pb.go"

sed -i'' \
    -e "${replace//#PKG#/plan}" 	\
    -e "${replace//#PKG#/ski}"		\
    -e "${replace//#PKG#/client}"   \
    -e "${replace//#PKG#/pdi}"		\
    -e "${replace//#PKG#/repo}" 	\
    "$DST_DIR/$PKG_NAME/$PKG_NAME.pb.go"
