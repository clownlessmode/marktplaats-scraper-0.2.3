package telegram

import (
	"context"

	"github.com/marktplaats-scraper/scraper-golang/internal/domain"
)

type StdoutNotifier struct{}

func (StdoutNotifier) NotifyUser(_ context.Context, _ domain.User, _ domain.Listing) error {
	return nil
}

var _ domain.Notifier = StdoutNotifier{}
