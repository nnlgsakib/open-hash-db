//go:build windows
// +build windows

package storage

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// GetAvailableSpace returns the available disk space in bytes for Windows
func (s *Storage) GetAvailableSpace() (int64, error) {
	var freeBytes, totalBytes, totalFreeBytes uint64
	path, err := windows.UTF16PtrFromString(s.dataPath)
	if err != nil {
		storageOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to convert path %s to UTF16: %w", s.dataPath, err)
	}

	err = windows.GetDiskFreeSpaceEx(path, &freeBytes, &totalBytes, &totalFreeBytes)
	if err != nil {
		storageOperationsTotal.WithLabelValues("get_space", "error").Inc()
		return 0, fmt.Errorf("failed to get disk stats for %s: %w", s.dataPath, err)
	}

	storageSpaceAvailable.Set(float64(freeBytes))
	storageOperationsTotal.WithLabelValues("get_space", "success").Inc()
	return int64(freeBytes), nil
}
