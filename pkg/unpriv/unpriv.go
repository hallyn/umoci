/*
 * umoci: Umoci Modifies Open Containers' Images
 * Copyright (C) 2016 SUSE LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unpriv

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cyphar/umoci/pkg/system"
)

// fiRestore restores the state given by an os.FileInfo instance at the given
// path by ensuring that an Lstat(path) will return as-close-to the same
// os.FileInfo.
func fiRestore(path string, fi os.FileInfo) {
	// archive/tar handles the OS-specific syscall stuff required to get atime
	// and mtime information for a file.
	hdr, _ := tar.FileInfoHeader(fi, "")

	// Apply the relevant information from the FileInfo.
	os.Chmod(path, fi.Mode())
	os.Chtimes(path, hdr.AccessTime, hdr.ModTime)
}

// splitpath splits the given path into each of the path components.
func splitpath(path string) []string {
	path = filepath.Clean(path)
	parts := strings.Split(path, string(os.PathSeparator))
	if filepath.IsAbs(path) {
		parts = append([]string{string(os.PathSeparator)}, parts...)
	}
	return parts
}

// isNotExist tells you if err is an error that implies that either the path
// accessed does not exist (or path components don't exist).
func isNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}

	// Check that it's not actually an ENOTDIR.
	perr, ok := err.(*os.PathError)
	if !ok {
		return false
	}
	errno, ok := perr.Err.(syscall.Errno)
	if !ok {
		return false
	}
	return errno == syscall.ENOTDIR || errno == syscall.ENOENT
}

// Wrap will wrap a given function, and call it in a context where all of the
// parent directories in the given path argument are such that the path can be
// resolved (you may need to make your own changes to the path to make it
// readable). Note that the provided function may be called several times, and
// if the error returned is such that !os.IsPermission(err), then no trickery
// will be performed. If fn returns an error, so will this function. All of the
// trickery is reverted when this function returns (which is when fn returns).
func Wrap(path string, fn func(path string) error) error {
	// FIXME: Should we be calling fn() here first?
	if err := fn(path); err == nil || !os.IsPermission(err) {
		return err
	}

	// We need to chown all of the path components we don't have execute rights
	// to. Specifically these are the path components which are parents of path
	// components we cannot stat. However, we must make sure to not touch the
	// path itself.
	parts := splitpath(filepath.Dir(path))
	start := len(parts)
	for {
		current := filepath.Join(parts[:start]...)
		_, err := os.Lstat(current)
		if err == nil {
			// We've hit the first element we can chown.
			break
		}
		if !os.IsPermission(err) {
			// This is a legitimate error.
			return fmt.Errorf("unpriv.Wrap %s: cannot lstat parent: %s", current, err)
		}
		start--
	}
	// Chown from the top down.
	for i := start; i <= len(parts); i++ {
		current := filepath.Join(parts[:i]...)
		fi, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("unpriv.Wrap %s: cannot lstat parent: %s", current, err)
		}
		// Add +rwx permissions to directories. If we have the access to change
		// the mode at all then we are the user owner (not just a group owner).
		if err := os.Chmod(current, fi.Mode()|0700); err != nil {
			return fmt.Errorf("unpriv.Wrap %s: cannot chmod parent: %s", current, err)
		}
		defer fiRestore(current, fi)
	}

	// Everything is wrapped. Return from this nightmare.
	return fn(path)
}

// Open is a wrapper around os.Open which has been wrapped with unpriv.Wrap to
// make it possible to open paths even if you do not currently have read
// permission. Note that the returned file handle references a path that you do
// not have read access to (since all changes are reverted when this function
// returns), so attempts to do Readdir() or similar functions that require
// doing lstat(2) may fail.
func Open(path string) (*os.File, error) {
	var fh *os.File
	err := Wrap(path, func(path string) error {
		// Get information so we can revert it.
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}

		// Add +r permissions to the file.
		if err := os.Chmod(path, fi.Mode()|0400); err != nil {
			return err
		}
		defer fiRestore(path, fi)

		// Open the damn thing.
		fh, err = os.Open(path)
		return err
	})
	return fh, err
}

// Create is a wrapper around os.Create which has been wrapped with unpriv.Wrap
// to make it possible to create paths even if you do not currently have read
// permission. Note that the returned file handle references a path that you do
// not have read access to (since all changes are reverted when this function
// returns).
func Create(path string) (*os.File, error) {
	var fh *os.File
	err := Wrap(path, func(path string) error {
		var err error
		fh, err = os.Create(path)
		return err
	})
	return fh, err
}

// Readdir is a wrapper around (*os.File).Readdir which has been wrapper with
// unpriv.Wrap to make it possible to get []os.FileInfo for the set of children
// of the provided directory path. The interface for this is quite different to
// (*os.File).Readdir because we have to have a proper filesystem path in order
// to get the set of child FileInfos (because all of the child paths need to be
// resolveable).
func Readdir(path string) ([]os.FileInfo, error) {
	var infos []os.FileInfo
	err := Wrap(path, func(path string) error {
		// Get information so we can revert it.
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}

		// Add +rx permissions to the file.
		if err := os.Chmod(path, fi.Mode()|0500); err != nil {
			return err
		}
		defer fiRestore(path, fi)

		// Open the damn thing.
		fh, err := os.Open(path)
		if err != nil {
			return err
		}
		defer fh.Close()

		// Get the set of dirents.
		infos, err = fh.Readdir(-1)
		return err
	})
	return infos, err
}

// Lstat is a wrapper around os.Lstat which has been wrapped with unpriv.Wrap
// to make it possible to get os.FileInfo about a path even if you do not
// currently have the required mode bits set to resolve the path. Note that you
// may not have resolve access after this function returns because all of the
// trickery is reverted by unpriv.Wrap.
func Lstat(path string) (os.FileInfo, error) {
	var fi os.FileInfo
	err := Wrap(path, func(path string) error {
		// Fairly simple.
		var err error
		fi, err = os.Lstat(path)
		return err
	})
	return fi, err
}

// Readlink is a wrapper around os.Readlink which has been wrapped with
// unpriv.Wrap to make it possible to get the linkname of a symlink even if you
// do not currently have teh required mode bits set to resolve the path. Note
// that you may not have resolve access after this function returns because all
// of this trickery is reverted by unpriv.Wrap.
func Readlink(path string) (string, error) {
	var linkname string
	err := Wrap(path, func(path string) error {
		// Fairly simple.
		var err error
		linkname, err = os.Readlink(path)
		return err
	})
	return linkname, err
}

// Symlink is a wrapper around os.Symlink which has been wrapped with
// unpriv.Wrap to make it possible to create a symlink even if you do not
// currently have the required access bits to create the symlink. Note that you
// may not have resolve access after this function returns because all of the
// trickery is reverted by unpriv.Wrap.
func Symlink(linkname, path string) error {
	return Wrap(path, func(path string) error {
		return os.Symlink(linkname, path)
	})
}

// Link is a wrapper around os.Link which has been wrapped with unpriv.Wrap to
// make it possible to create a hard link even if you do not currently have the
// required access bits to create the hard link. Note that you may not have
// resolve access after this function returns because all of the trickery is
// reverted by unpriv.Wrap.
func Link(linkname, path string) error {
	return Wrap(path, func(path string) error {
		// We have to double-wrap this, because you need search access to the
		// linkname. This is safe because any common ancestors will be reverted
		// in reverse call stack order.
		return Wrap(linkname, func(linkname string) error {
			return os.Link(linkname, path)
		})
	})
}

// Chmod is a wrapper around os.Chmod which has been wrapped with unpriv.Wrap
// to make it possible to change the permission bits of a path even if you do
// not currently have the required access bits to access the path.
func Chmod(path string, mode os.FileMode) error {
	return Wrap(path, func(path string) error {
		return os.Chmod(path, mode)
	})
}

// Lchown is a wrapper around os.Lchown which has been wrapped with unpriv.Wrap
// to make it possible to change the owner of a path even if you do not
// currently have the required access bits to access the path. Note that this
// function is not particularly useful in most rootless scenarios.
//
// FIXME: This probably should be removed because it's questionably useful.
func Lchown(path string, uid, gid int) error {
	return Wrap(path, func(path string) error {
		return os.Lchown(path, uid, gid)
	})
}

// Chtimes is a wrapper around os.Chtimes which has been wrapped with
// unpriv.Wrap to make it possible to change the modified times of a path even
// if you do not currently have the required access bits to access the path.
func Chtimes(path string, atime, mtime time.Time) error {
	return Wrap(path, func(path string) error {
		return os.Chtimes(path, atime, mtime)
	})
}

// Lutimes is a wrapper around system.Lutimes which has been wrapped with
// unpriv.Wrap to make it possible to change the modified times of a path even
// if you do no currently have the required access bits to access the path.
func Lutimes(path string, atime, mtime time.Time) error {
	return Wrap(path, func(path string) error {
		return system.Lutimes(path, atime, mtime)
	})
}

// Remove is a wrapper around os.Remove which has been wrapped with unpriv.Wrap
// to make it possible to remove a path even if you do not currently have the
// required access bits to modify or resolve the path.
func Remove(path string) error {
	return Wrap(path, os.Remove)
}

// RemoveAll is similar to os.RemoveAll but in order to implement it properly
// all of the internal functions were wrapped with unpriv.Wrap to make it
// possible to remove a path (even if it has child paths) even if you do not
// currently have enough access bits.
func RemoveAll(path string) error {
	return Wrap(path, func(path string) error {
		// If remove works, we're done.
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}

		// Is this a directory?
		fi, serr := os.Lstat(path)
		if serr != nil {
			if isNotExist(serr) {
				serr = nil
			}
			return serr
		}
		// Return error from remove if it's not a directory.
		if !fi.IsDir() {
			return err
		}

		// Open the directory.
		fd, err := Open(path)
		if err != nil {
			// We hit a race, but don't worry about it.
			if os.IsNotExist(err) {
				err = nil
			}
			return err
		}

		// We need to change the mode to Readdirnames. We don't need to worry
		// about permissions because we're already in a context with
		// filepath.Dir(path) is writeable.
		os.Chmod(path, fi.Mode()|0400)
		defer fiRestore(path, fi)

		// Remove contents recursively.
		err = nil
		for {
			names, err1 := fd.Readdirnames(128)
			for _, name := range names {
				err1 := RemoveAll(filepath.Join(path, name))
				if err == nil {
					err = err1
				}
			}
			if err1 == io.EOF {
				break
			}
			if err == nil {
				err = err1
			}
			if len(names) == 0 {
				break
			}
		}

		// Close the directory.
		fd.Close()

		// Remove the directory. This should now work.
		err1 := os.Remove(path)
		if err1 == nil || os.IsNotExist(err1) {
			return nil
		}
		if err == nil {
			err = err1
		}
		return err
	})
}