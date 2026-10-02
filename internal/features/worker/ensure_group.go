package worker

import (
	"context"
	"fmt"
	"strings"
)

// EnsureGroup создаёт consumer group, если её нет. Группа читает стрим с
// начала ("0"), а не с конца ("$"): если стрим пересоздан без группы
// (например, Redis перезапущен без тома), алерты, успевшие попасть в него
// до восстановления группы, иначе были бы пропущены. Повторная обработка
// безопасна: processMessage пропускает терминальные и исчезнувшие алерты.
func (w *Worker) EnsureGroup(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, w.stream, w.group, "0")
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create group: %w", err)
	}
	return nil
}
