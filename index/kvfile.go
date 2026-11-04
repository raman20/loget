package index

// minFileRun is the default KVOptions.MinFileRun. A var so tests can put
// every run in a file.
var minFileRun = 1 << 20

// fileBuf is how many bytes a file builder buffers before writing.
const fileBuf = 1 << 20

// RunFiles gives a KV memory outside the Go heap for its runs, the
// immutable compressed part of the index (design 21): one file per run,
// written once, then mapped read-only. The files are a cache of state
// derived from the log; nothing reads them across processes.
// internal/runfile implements it with unlinked, memory-mapped files.
type RunFiles interface {
	Create() (RunFile, error)
}

// RunFile is the file of one run.
type RunFile interface {
	// Write appends p.
	Write(p []byte) error
	// Map extends the file by slots zeroed uint64s (8-aligned) and maps
	// it writable: it returns the bytes written and the slots.
	Map(slots int) ([]byte, []uint64, error)
	// Seal makes the mapping read-only.
	Seal() error
	// Release unmaps and closes the file; idempotent. The KV calls it when
	// the run is replaced or its build abandoned, under the owner's write
	// lock; an implementation also releases a file that becomes
	// unreachable (a KV dropped whole), which is why every use of run
	// memory keeps its run alive (runtime.KeepAlive).
	Release()
}

// finishFile completes a file builder: maps the file with room for the
// slots, fills them in place and seals it. On error the file is released.
func (w *builder) finishFile(r *run) (*run, error) {
	w.flush()
	f, err := w.f, w.err
	w.f, w.buf = nil, nil
	if err == nil {
		r.b, r.slots, err = f.Map(nslots(r.n))
	}
	if err == nil {
		r.fillSlots(f, w.par)
		err = f.Seal()
	}
	if err != nil {
		f.Release()
		r.b, r.slots = nil, nil
		return nil, err
	}
	r.mp = f
	return r, nil
}

// abort discards a builder that will not be finished.
func (w *builder) abort() {
	if w.f != nil {
		w.f.Release()
		w.f = nil
	}
	w.r, w.buf = nil, nil
}

// flush writes the buffered bytes of a file builder. After an error it
// keeps counting bytes (offsets stay right) but writes nothing more.
func (w *builder) flush() {
	if w.f == nil || len(w.buf) == 0 {
		return
	}
	if w.err == nil {
		w.err = w.f.Write(w.buf)
	}
	w.off += len(w.buf)
	w.buf = w.buf[:0]
}

// newFileBuilder returns a builder writing to a new run file.
func newFileBuilder(fs RunFiles) (*builder, error) {
	f, err := fs.Create()
	if err != nil {
		return nil, err
	}
	w := newBuilder()
	w.f, w.buf = f, make([]byte, 0, fileBuf+fileBuf/4)
	return w, nil
}
