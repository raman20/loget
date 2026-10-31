package store

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
)

// BatchDeleter is a store that deletes many objects for less than one
// Delete each (design 23): the journal writes one delete record and syncs
// twice per batch instead of twice per object; the file layout syncs each
// directory once.
type BatchDeleter interface {
	// DeleteBatch deletes keys; missing keys are not an error. On error
	// any subset of the keys may be deleted.
	DeleteBatch(ctx context.Context, keys []string) error
}

// DeleteBatch deletes keys through st's BatchDeleter, else one Delete at
// a time. Missing keys are not an error; on error any subset of the keys
// may be deleted. A store wrapper must forward DeleteBatch (with this
// function), or the batch is lost to a loop of Deletes.
func DeleteBatch(ctx context.Context, st Store, keys []string) error {
	if b, ok := st.(BatchDeleter); ok {
		return b.DeleteBatch(ctx, keys)
	}
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := st.Delete(ctx, k); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	return nil
}

// DeleteBatch deletes keys under one lock.
func (s *Mem) DeleteBatch(_ context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.m, k)
	}
	return nil
}

// DeleteBatch removes the files, then syncs each directory once.
func (s *FS) DeleteBatch(_ context.Context, keys []string) error {
	dirs := map[string]bool{}
	for _, k := range keys {
		p, err := s.path(k)
		if err != nil {
			return err
		}
		s.fds.drop(p)
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		dirs[filepath.Dir(p)] = true
	}
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		if err := s.syncDir(d); err != nil {
			return err
		}
	}
	return nil
}
