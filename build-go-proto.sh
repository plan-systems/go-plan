
#!/bin/sh
#
# See:
#    plan-systems/plan-protobuf/README.md 
#    http://plan-systems.org
#
#

if [[ $# -ne 2 ]]; then
    echo "Usage: ./build-go-proto.sh <file>[.proto] <out_path>"
    exit
fi

PKG="$1"
PROTO_FILE="$1.proto"
DST_DIR="$2"

BUILD_PROTO="../plan-protobufs/build-proto.sh"


FIND="\"github.com\/plan-systems\/plan-protobufs\/proto\""
REPLACE="\"github.com\/plan-systems\/plan-protobufs\/proto\""

replace='s/#PKG# \"github.com\/plan-systems\/plan-protobufs\/proto\"/#PKG# \"github.com\/plan-systems\/plan-core\/#PKG#\"/' 


$BUILD_PROTO "$PROTO_FILE" gofast "$DST_DIR"
sed -i ''	-e "${replace//#PKG#/plan}" 	\
			-e "${replace//#PKG#/ski}"		\
			-e "${replace//#PKG#/pdi}"		\
			-e "${replace//#PKG#/repo}" 	\
			"$DST_DIR/$PKG.pb.go"

