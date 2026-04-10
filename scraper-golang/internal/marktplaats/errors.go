package marktplaats

import (
	"errors"
	"fmt"
)

var (
	ErrForbidden       = errors.New("marktplaats: доступ запрещён или слишком много запросов")
	ErrNextDataMissing = errors.New("marktplaats: не найден __NEXT_DATA__")
	ErrBlockedPage     = errors.New("marktplaats: страница похожа на блокировку или пустая")
	ErrListingTooOld   = errors.New("marktplaats: объявление старше max_age_hours")
)

// ListingTooOldDetails оборачивает ErrListingTooOld с данными для логов/аудита.
type ListingTooOldDetails struct {
	ItemID     string
	Title      string
	ListingURL string
	ListedTS   string
	AgeHours   float64
	MaxHours   float64
}

func (e *ListingTooOldDetails) Error() string {
	if e == nil {
		return ErrListingTooOld.Error()
	}
	return fmt.Sprintf("marktplaats: объявление старше max_age_hours (%.1f ч > %.1f ч)", e.AgeHours, e.MaxHours)
}

func (e *ListingTooOldDetails) Unwrap() error { return ErrListingTooOld }

// CategoryStaleError matches Python CategoryStale: stop category, keep collected listings.
type CategoryStaleError struct {
	Listings []Listing
	Msg      string
}

func (e *CategoryStaleError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return "marktplaats: категория устарела (объявление слишком старое)"
}

// UnexpectedCategoryIDError matches Python UnexpectedCategoryId.
type UnexpectedCategoryIDError struct {
	Got  int
	Want int
}

func (e *UnexpectedCategoryIDError) Error() string {
	return fmt.Sprintf("marktplaats: неожиданный id дочерней категории (получено %d, ожидалось %d)", e.Got, e.Want)
}
