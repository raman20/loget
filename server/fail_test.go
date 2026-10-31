package server

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/raman20/loget"
)

// Retryable conditions must not surface as 500.
func TestFailCodes(t *testing.T) {
	for err, want := range map[error]int{
		loget.ErrBusy:                      409,
		fmt.Errorf("x: %w", loget.ErrBusy): 409,
		loget.ErrFenced:                    503,
		loget.ErrNotFound:                  404,
		loget.ErrTooLarge:                  413,
		fmt.Errorf("%w: bad", errBad):      400,
	} {
		w := httptest.NewRecorder()
		fail(w, err)
		if w.Code != want {
			t.Errorf("%v: %d, want %d", err, w.Code, want)
		}
	}
}
