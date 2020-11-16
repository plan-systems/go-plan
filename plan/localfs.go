package plan

import (
	"os"
	"path"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	"github.com/plan-systems/plan-go/bufs"
)

var (
	// DefaultFileMode is used to express the default mode of file creation.
	DefaultFileMode = os.FileMode(0775)
)

// ExpandAndCheckPath parses/expands the given path and then verifies it's existence or non-existence,
// depending on autoCreate and returning the the expanded path.
//
// If autoCreate == true, an error is returned if the dir didn't exist and failed to be created.
//
// If autoCreate == false, an error is returned if the dir doesn't exist.
func ExpandAndCheckPath(
	dirPath string,
	autoCreate bool,
) (string, error) {

	pathname, err := homedir.Expand(dirPath)
	if err != nil {
		err = errors.Errorf("error expanding '%s'", dirPath)
	} else {
		_, err = os.Stat(pathname)
		if err != nil && os.IsNotExist(err) {
			if autoCreate {
				err = os.MkdirAll(pathname, DefaultFileMode)
			} else {
				err = errors.Errorf("path '%s' does not exist", pathname)
			}
		} else if err == nil {
			// if cannotExist {
			// 	err = errors.Errorf("for safety, path '%s' must not already exist", pathname)
			// }
		}
	}

	if err != nil {
		return "", err
	}

	return pathname, nil
}

// CreateNewDir creates the specified dir (and returns an error if the dir already exists)
//
// If dirPath is absolute then basePath is ignored.
// Returns the effective pathname.
func CreateNewDir(basePath, dirPath string) (string, error) {
	var pathname string

	if path.IsAbs(dirPath) {
		pathname = dirPath
	} else {
		pathname = path.Join(basePath, dirPath)
	}

	if _, err := os.Stat(pathname); !os.IsNotExist(err) {
		return "", errors.Errorf("for safety, the path '%s' must not already exist", pathname)
	}

	if err := os.MkdirAll(pathname, DefaultFileMode); err != nil {
		return "", err
	}

	return pathname, nil
}

var remapCharset = map[rune]rune{
	' ':  '-',
	'.':  '-',
	'?':  '-',
	'\\': '+',
	'/':  '+',
	'&':  '+',
}

// MakeFSFriendly makes a given string safe to use for a file system.
// If suffix is given, the hex encoding of those bytes are appended after a space.
func MakeFSFriendly(name string, suffix []byte) string {

	var b strings.Builder
	for _, r := range name {
		if replace, ok := remapCharset[r]; ok {
			if replace != 0 {
				b.WriteRune(replace)
			}
		} else {
			b.WriteRune(r)
		}
	}

	if len(suffix) > 0 {
		b.WriteString(" ")
		b.WriteString(bufs.Base32Encoding.EncodeToString(suffix))
	}

	friendlyName := b.String()
	return friendlyName
}
