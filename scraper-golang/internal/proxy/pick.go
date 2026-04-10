package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// PickFirstWorking возвращает первый прокси, прошедший Check.
//
// concurrency <= 1: проверки строго по порядку; при ошибке вызывается onFail (если не nil).
// concurrency > 1: параллельный пул; при первом успехе остальные проверки отменяются по ctx;
// onFail не вызывается (иначе сотни строк в лог).
func PickFirstWorking(ctx context.Context, entries []Entry, probeURL string, concurrency int, onFail func(Entry, error)) (*Entry, error) {
	if len(entries) == 0 {
		return nil, errors.New("пустой список прокси")
	}
	if concurrency <= 1 {
		for i := range entries {
			e := &entries[i]
			if err := Check(ctx, *e, probeURL); err != nil {
				if onFail != nil {
					onFail(*e, err)
				}
				continue
			}
			return e, nil
		}
		return nil, fmt.Errorf("нет рабочего прокси")
	}

	n := concurrency
	if n > len(entries) {
		n = len(entries)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int, len(entries))
	for i := range entries {
		jobs <- i
	}
	close(jobs)

	var mu sync.Mutex
	var chosen *Entry
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for i := range jobs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			mu.Lock()
			if chosen != nil {
				mu.Unlock()
				return
			}
			mu.Unlock()

			e := &entries[i]
			if err := Check(ctx, *e, probeURL); err != nil {
				continue
			}
			mu.Lock()
			if chosen == nil {
				chosen = e
				cancel()
			}
			mu.Unlock()
		}
	}

	for w := 0; w < n; w++ {
		wg.Add(1)
		go worker()
	}
	wg.Wait()

	if chosen == nil {
		return nil, fmt.Errorf("нет рабочего прокси")
	}
	return chosen, nil
}
