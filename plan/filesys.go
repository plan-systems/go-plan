package plan

import (
	"os"
	"path"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
)

var (
	// DefaultFileMode is used to express the default mode of file creation.
	DefaultFileMode = os.FileMode(0775)
)

// SetupBaseDir parses/expands inLocalPath and then verifies it's existence or non-existence,
// depending on inCreate and returning the the expanded path.
//
// If inCreate == true, an error is returned if the dir exists or failed to be created.
//
// If inCreate == false, an error is returned if the die doesn't exist.
func SetupBaseDir(
	inLocalPath string,
	inCreate bool,
) (string, error) {

	pathname, err := homedir.Expand(inLocalPath)
	if err != nil {
		err = errors.Errorf("error expanding '%s'", inLocalPath)
	} else {
		_, err = os.Stat(pathname)
		if err != nil && os.IsNotExist(err) {
			if inCreate {
				err = os.MkdirAll(pathname, DefaultFileMode)
			} else {
				err = errors.Errorf("path '%s' does not exist", pathname)
			}
		} else if err == nil {
			if inCreate {
				err = errors.Errorf("for safety, path '%s' must not already exist", pathname)
			}
		}
	}

	if err != nil {
		return "", err
	}

	return pathname, nil
}

// CreateNewDir creates the specified dir (and returns an error if the dir already exists)
//
// If inPath is absolute then inBasePath is ignored.
// Returns the effective pathname.
func CreateNewDir(inBasePath, inPath string) (string, error) {
	var pathname string

	if path.IsAbs(inPath) {
		pathname = inPath
	} else {
		pathname = path.Join(inBasePath, inPath)
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
// If inSuffix is given, the hex encoding of those bytes are appended after a space.
func MakeFSFriendly(inName string, inSuffix []byte) string {

	var b strings.Builder
	for _, r := range inName {
		if replace, ok := remapCharset[r]; ok {
			if replace != 0 {
				b.WriteRune(replace)
			}
		} else {
			b.WriteRune(r)
		}
	}

	if len(inSuffix) > 0 {
		b.WriteString(" ")
		b.WriteString(BinEncode(inSuffix))
	}

	name := b.String()

	return name
}
