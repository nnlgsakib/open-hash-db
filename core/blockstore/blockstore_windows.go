//go:build windows
// +build windows

package blockstore

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// GetAvailableSpace returns the available disk space in bytes for Windows
func (bs *Blockstore) GetAvailableSpace() (int64, error) {
	var freeBytes, totalBytes, totalFreeBytes uint64
	path, err := windows.UTF16PtrFromString(bs.rootPath)
	if err != nil {
		blockstoreOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to convert path %s to UTF16: %w", bs.rootPath, err)
	}

	err = windows.GetDiskFreeSpaceEx(path, &freeBytes, &totalBytes, &totalFreeBytes)
	if err != nil {
		blockstoreOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to get disk stats for %s: %w", bs.rootPath, err)
	}

	blockstoreSpaceAvailable.Set(float64(freeBytes))
	blockstoreOperationsTotal.WithLabelValues("get_space", "success").Inc()
	return int64(freeBytes), nil
}
