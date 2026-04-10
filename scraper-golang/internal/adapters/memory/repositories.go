package memory

import (
	"context"

	"github.com/marktplaats-scraper/scraper-golang/internal/domain"
)

type WatchRepository struct{}

func (WatchRepository) Get(_ context.Context, _ string) (domain.Watch, error) {
	return domain.Watch{}, nil
}

type JobQueue struct{}

func (JobQueue) Enqueue(_ context.Context, _ domain.ScrapeJob) error {
	return nil
}

var (
	_ domain.WatchRepository = WatchRepository{}
	_ domain.JobQueue        = JobQueue{}
)
