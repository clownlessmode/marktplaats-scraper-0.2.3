package domain

import "context"

type WatchRepository interface {
	Get(ctx context.Context, id string) (Watch, error)
}

type JobQueue interface {
	Enqueue(ctx context.Context, job ScrapeJob) error
}

type Notifier interface {
	NotifyUser(ctx context.Context, user User, listing Listing) error
}
