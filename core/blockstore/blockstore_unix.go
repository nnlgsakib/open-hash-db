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
	if err := syscall.Statfs(bs.dataPath, &stat); err != nil {
		blockstoreOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to get disk stats for %s: %w", bs.dataPath, err)
	}

	// Calculate available space (blocks available * block size)
	available := int64(stat.Bavail) * int64(stat.Bsize)
	blockstoreSpaceAvailable.Set(float64(available))
	blockstoreOperationsTotal.WithLabelValues("get_space", "success").Inc()
	return available, nil
}
