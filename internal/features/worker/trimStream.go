package worker

import (
	"context"
	"fmt"
	"time"
)

func (w *Worker) trimStream(ctx context.Context) error {
	cutoff := time.Now().Add(-trimMaxAge).UnixMilli()
	minID := fmt.Sprintf("%d-0", cutoff)
	if err := w.rdb.XTrim(ctx, w.stream, minID); err != nil {
		return fmt.Errorf("xtrim: %w", err)
	}
	return nil
}
