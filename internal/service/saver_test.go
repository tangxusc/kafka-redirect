package service

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileSaverEnforceLimit(t *testing.T) {
	dir := t.TempDir()

	saver, err := NewFileSaver(dir, 350)
	if err != nil {
		t.Fatalf("NewFileSaver() error = %v", err)
	}
	defer saver.Close()

	value := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 80)))
	for i := 0; i < 8; i++ {
		record := MessageRecord{
			Topic:     "source-topic",
			Value:     value,
			Timestamp: time.Unix(int64(i), 0).UTC(),
		}
		if err := saver.Save(record); err != nil {
			t.Fatalf("Save() error on round %d: %v", i, err)
		}
	}

	totalSize, fileCount := jsonlStats(t, dir)
	if fileCount == 0 {
		t.Fatalf("jsonl file count = 0, want > 0")
	}
	if totalSize > 350 {
		t.Fatalf("jsonl total size = %d, want <= 350", totalSize)
	}
}

func TestFileSaverTooLargeRecord(t *testing.T) {
	dir := t.TempDir()

	saver, err := NewFileSaver(dir, 120)
	if err != nil {
		t.Fatalf("NewFileSaver() error = %v", err)
	}
	defer saver.Close()

	record := MessageRecord{
		Topic:     "source-topic",
		Value:     base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 200))),
		Timestamp: time.Now().UTC(),
	}
	if err := saver.Save(record); err == nil {
		t.Fatalf("Save() error = nil, want error for oversized record")
	}
}

func jsonlStats(t *testing.T, dir string) (int64, int) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	total := int64(0)
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			t.Fatalf("entry.Info() error = %v", infoErr)
		}
		total += info.Size()
		count++
	}
	return total, count
}
