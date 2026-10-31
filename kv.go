package loget

import "context"

// KV is the built-in index: a record store on space "kv". It has no state
// of its own: its records, kept with every version in the engine's MVCC
// record store, are its state. It uses only the record API that any index
// may use (records.go).

const spaceKV = "kv"

// KV is one key and its value.
type KV = Record

// Put sets key = val.
func (tx *Tx) Put(key string, val []byte, opts ...WriteOpt) error {
	return tx.PutRecord(spaceKV, key, val, opts...)
}

// Delete deletes key (a no-op if it is absent).
func (tx *Tx) Delete(key string) error { return tx.DeleteRecord(spaceKV, key) }

// Get returns key's value at the snapshot, or ErrNotFound.
func (tx *Tx) Get(key string) ([]byte, error) { return tx.GetRecord(spaceKV, key) }

// Scan returns live keys with prefix (sorted), including this tx's writes.
func (tx *Tx) Scan(prefix string) ([]KV, error) { return tx.ScanRecords(spaceKV, prefix) }

// Put sets key = val on main in its own transaction.
func (db *DB) Put(ctx context.Context, key string, val []byte, opts ...WriteOpt) error {
	return db.Update(ctx, func(tx *Tx) error { return tx.Put(key, val, opts...) })
}

// Get reads key on main.
func (db *DB) Get(ctx context.Context, key string) (v []byte, err error) {
	err = db.View(ctx, func(tx *Tx) error { v, err = tx.Get(key); return err })
	return
}
