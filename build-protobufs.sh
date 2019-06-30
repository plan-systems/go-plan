
#!/bin/sh
#
# See:
#    plan-systems/plan-protobuf/README.md 
#    http://plan-systems.org
#
#


BUILD_GO_ROTO="./build-go-proto.sh"
SELF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"


PKGS=(
    "plan" 
    "ski" 
    "pdi"
    "repo"
	"client"
)
NUM_PKGS=$(( ${#PKGS[@]} ))

# Generate language-specific source files for each .proto file
for (( i=0; i<$NUM_PKGS; i++ ));
do

	PKG=${PKGS[$i]}

	DST_DIR="$SELF_DIR/$PKG"
	$BUILD_GO_ROTO "$PKG" "$DST_DIR"

done

