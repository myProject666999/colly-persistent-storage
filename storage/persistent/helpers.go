package persistent

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

func sha256Hex(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

// writeFileAtomic writes data to path via a temp file in the same directory
// followed by an atomic rename, and fsyncs the file and directory so the
// data survives a kill -9 / power loss once the call returns successfully.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	// fsync the directory so the rename itself is durable.
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = df.Sync()
	df.Close()
	return err
}

// removeFile deletes a file ignoring "not found" errors and fsyncs the
// parent directory.
func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	dir := filepath.Dir(path)
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = df.Sync()
	df.Close()
	return err
}
