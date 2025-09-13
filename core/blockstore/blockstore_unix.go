//go:build linux || darwin || freebsd || openbsd || netbsd
// +build linux darwin freebsd openbsd netbsd

package blockstore

import (
    "fmt"

    "syscall"
)

// GetAvailableSpace returns the available disk space in bytes for Unix-like systems
func (bs *Blockstore) GetAvailableSpace() (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(bs.rootPath, &stat); err != nil {
		blockstoreOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to get disk stats for %s: %w", bs.rootPath, err)
	}

	// Calculate available space (blocks available * block size)
	available := int64(stat.Bavail) * int64(stat.Bsize)
	blockstoreSpaceAvailable.Set(float64(available))
	blockstoreOperationsTotal.WithLabelValues("get_space", "success").Inc()
	return available, nil
}

// syncDir performs an fsync on the directory to ensure metadata durability.
func (bs *Blockstore) syncDir(dir string) error {
    fd, err := syscall.Open(dir, syscall.O_RDONLY, 0)
    if err != nil {
        return err
    }
    defer syscall.Close(fd)
    return syscall.Fsync(fd)
}
