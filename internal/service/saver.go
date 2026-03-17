package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type FileSaver struct {
	dir         string
	maxBytes    int64
	mu          sync.Mutex
	file        *os.File
	currentPath string
	currentSize int64
	sequence    uint64
}

type savedFile struct {
	path    string
	size    int64
	modTime time.Time
}

func NewFileSaver(dir string, maxBytes int64) (*FileSaver, error) {
	cleanDir := strings.TrimSpace(dir)
	if cleanDir == "" {
		return nil, errors.New("save directory is empty")
	}
	if maxBytes < 0 {
		return nil, errors.New("maxBytes cannot be negative")
	}

	if err := os.MkdirAll(cleanDir, 0o755); err != nil {
		return nil, fmt.Errorf("create save directory: %w", err)
	}

	return &FileSaver{
		dir:      cleanDir,
		maxBytes: maxBytes,
	}, nil
}

func (s *FileSaver) Save(record MessageRecord) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal message record: %w", err)
	}
	line = append(line, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	lineSize := int64(len(line))
	if s.maxBytes > 0 && lineSize > s.maxBytes {
		return fmt.Errorf("single record size %d exceeds max_size %d", lineSize, s.maxBytes)
	}

	if s.file == nil {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}

	if s.maxBytes > 0 && s.currentSize+lineSize > s.maxBytes {
		if err := s.rotateLocked(); err != nil {
			return err
		}
	}

	n, writeErr := s.file.Write(line)
	if writeErr != nil {
		return fmt.Errorf("write save file: %w", writeErr)
	}
	s.currentSize += int64(n)

	if s.maxBytes > 0 {
		if err := s.enforceLimitLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileSaver) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	s.currentPath = ""
	s.currentSize = 0
	return err
}

func (s *FileSaver) rotateLocked() error {
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("close current save file: %w", err)
		}
	}

	fileName := fmt.Sprintf(
		"messages-%s-%04d.jsonl",
		time.Now().UTC().Format("20060102-150405.000"),
		s.sequence,
	)
	s.sequence++

	fullPath := filepath.Join(s.dir, fileName)
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open save file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat save file: %w", err)
	}

	s.file = file
	s.currentPath = fullPath
	s.currentSize = stat.Size()
	return nil
}

func (s *FileSaver) enforceLimitLocked() error {
	files, err := s.listSavedFilesLocked()
	if err != nil {
		return err
	}

	total := int64(0)
	for _, f := range files {
		total += f.size
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].path < files[j].path
		}
		return files[i].modTime.Before(files[j].modTime)
	})

	for total > s.maxBytes && len(files) > 0 {
		oldest := files[0]
		files = files[1:]

		if oldest.path == s.currentPath {
			if len(files) == 0 {
				break
			}
			files = append(files, oldest)
			continue
		}

		if err := os.Remove(oldest.path); err != nil {
			return fmt.Errorf("remove old save file %s: %w", oldest.path, err)
		}
		total -= oldest.size
	}

	return nil
}

func (s *FileSaver) listSavedFilesLocked() ([]savedFile, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read save directory: %w", err)
	}

	files := make([]savedFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(entry.Name()), ".jsonl") {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil, fmt.Errorf("stat save file %s: %w", entry.Name(), infoErr)
		}
		files = append(files, savedFile{
			path:    filepath.Join(s.dir, entry.Name()),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}
	return files, nil
}
