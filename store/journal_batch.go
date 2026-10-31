//go:build unix

package store

import (
	"context"
	"encoding/binary"
	"fmt"
)

// jrnMaxBatch caps the key list of one batch delete record. A batch is one
// record so one checksum covers it: a torn batch is a torn tail, never a
// damaged record followed by valid ones (which recovery must call
// corruption). Small enough that its write is cheap next to the syncs.
var jrnMaxBatch = 1 << 20

// DeleteBatch deletes keys with one batch record per up to jrnMaxBatch
// bytes of keys: append the record and sync, erase every deleted put,
// sync again (design 23). Before, each key cost both syncs. A journal
// file of version 1 (made by an older build) keeps one record per key, so
// that build can still open it.
func (j *Journal) DeleteBatch(ctx context.Context, keys []string) error {
	for _, k := range keys {
		if err := checkKey(k); err != nil {
			return err
		}
	}
	if j.ver < 2 {
		for _, k := range keys {
			if err := j.Delete(ctx, k); err != nil {
				return err
			}
		}
		return nil
	}
	for len(keys) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, size := 0, 0
		for n < len(keys) && (n == 0 || size+2+len(keys[n]) <= jrnMaxBatch) {
			size += 2 + len(keys[n])
			n++
		}
		if err := j.deleteBatch(keys[:n]); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

func (j *Journal) deleteBatch(keys []string) error {
	unlock, err := j.lockWrite()
	if err != nil {
		return err
	}
	defer unlock()
	var es []jent
	var data []byte
	seen := map[string]bool{}
	j.mu.RLock()
	for _, k := range keys {
		if e, ok := j.idx[k]; ok && !seen[k] {
			seen[k] = true
			es = append(es, e)
			data = binary.LittleEndian.AppendUint16(data, uint16(len(k)))
			data = append(data, k...)
		}
	}
	at := j.end
	j.mu.RUnlock()
	if len(es) == 0 {
		return nil
	}
	rec := j.record(at, recDels, delsKey, data)
	if err := j.appendRec(rec, nil); err != nil {
		return err
	}
	j.mu.Lock()
	for k := range seen {
		delete(j.idx, k)
	}
	j.end = at + int64(len(rec))
	j.mu.Unlock()
	for _, e := range es {
		if err := j.erase(e); err != nil {
			return err
		}
	}
	return j.sync()
}

// delsKey is the key field of a batch delete record (the keys are in its
// data).
const delsKey = "\x00dels"

// eachDel calls fn for every key a valid batch record's data lists, or
// returns an error if the list is malformed.
func eachDel(data []byte, fn func(key string)) error {
	for len(data) > 0 {
		if len(data) < 2 {
			return fmt.Errorf("store: batch delete record: short length")
		}
		n := int(binary.LittleEndian.Uint16(data))
		if n == 0 || n > jrnMaxKey || 2+n > len(data) {
			return fmt.Errorf("store: batch delete record: bad key length %d", n)
		}
		fn(string(data[2 : 2+n]))
		data = data[2+n:]
	}
	return nil
}
