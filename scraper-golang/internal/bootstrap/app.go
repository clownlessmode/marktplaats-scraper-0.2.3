package bootstrap

import (
	"github.com/marktplaats-scraper/scraper-golang/internal/adminbot"
	"github.com/marktplaats-scraper/scraper-golang/internal/clientbot"
)

func RunAdminBot() { adminbot.RunCLI() }

func RunClientBot() { clientbot.RunCLI() }
