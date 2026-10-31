package store

// JournalVersion is the journal file format this build writes (new files;
// version 2 adds batch delete records, design 23). It reads 1 and 2 and
// keeps writing a version 1 file as version 1.
// It lives outside journal.go (unix only) so every platform can report it.
const JournalVersion = 2
