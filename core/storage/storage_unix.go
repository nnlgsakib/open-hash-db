//go:build linux || darwin || freebsd || openbsd || netbsd
// +build linux darwin freebsd openbsd netbsd

package storage

import (
	"fmt"

	"syscall"
)

// GetAvailableSpace returns the available disk space in bytes for Unix-like systems
func (s *Storage) GetAvailableSpace() (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.dataPath, &stat); err != nil {
		storageOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to get disk stats for %s: %w", s.dataPath, err)
	}

	// Calculate available space (blocks available * block size)
	available := int64(stat.Bavail) * int64(stat.Bsize)
	storageSpaceAvailable.Set(float64(available))
	storageOperationsTotal.WithLabelValues("get_space", "success").Inc()
	return available, nil
}
