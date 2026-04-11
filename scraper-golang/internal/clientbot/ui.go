package clientbot

import (
	"database/sql"
	"fmt"
	"html"
	"strings"
	"unicode/utf8"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/adminbot"
	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
)

const emailsPerPage = 15

func pendingRegText() string {
	return "📩 <b>Заявка на регистрацию отправлена администратору</b>\n\n" +
		"────────────────────────────────\n" +
		"📋 <b>Ваш статус:</b> <i>Не подтверждён</i>\n" +
		"────────────────────────────────\n\n" +
		"Ожидайте подтверждения. Администратор одобрит или отклонит заявку.\n\n" +
		"Если заявка отклонена — бот больше не будет отвечать."
}

func workerKB(onShift bool) tgbotapi.InlineKeyboardMarkup {
	base := [][]tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📦 Товары сегодня", "list_today")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📧 Почты", "worker_emails")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📝 Шаблоны", "worker_templates")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📤 Рассылка", "worker_bulk_mail")),
	}
	if onShift {
		top := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("🛑 Закрыть смену", "shift_stop"))
		return tgbotapi.NewInlineKeyboardMarkup(append([][]tgbotapi.InlineKeyboardButton{top}, base...)...)
	}
	top := tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("▶️ Начать смену", "shift_start"))
	return tgbotapi.NewInlineKeyboardMarkup(append([][]tgbotapi.InlineKeyboardButton{top}, base...)...)
}

func workerEmailsKB(db *sql.DB, userID int64) tgbotapi.InlineKeyboardMarkup {
	n, _ := listingsdb.EmailsTotalCount(db, userID)
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("➕ Добавить (mail:apppassword)", "worker_emails_add")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📤 Загрузить CSV", "worker_emails_upload")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("📋 Список (%d)", n), "worker_emails_list_0")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "worker_main")),
	)
}

func workerEmailsAddHTML() string {
	return "➕ <b>Добавить почты</b>\n\nТолько Gmail. Формат:\n<code>mail@gmail.com:apppassword</code>\n\n" +
		"App Password: myaccount.google.com/apppasswords\n\n" +
		"Несколько строк — через Enter. Разделители: <code>:</code> <code>;</code> <code>Tab</code>"
}

func workerEmailsUploadHTML() string {
	return "📤 <b>Загрузить CSV</b>\n\nКолонки: email, apppassword (или почта / пароль)\n\nПришлите файл .csv"
}

func workerTplAddHTML() string {
	var help strings.Builder
	help.WriteString("<b>Доступные переменные:</b>\n")
	for k, desc := range adminbot.TemplateVarDescriptions {
		help.WriteString(fmt.Sprintf("• <code>{%s}</code> — %s\n", k, html.EscapeString(desc)))
	}
	ex := adminbot.TemplateExampleBody()
	filled := adminbot.FormatTemplateExampleBody(ex)
	exSubj := adminbot.TemplateExampleSubject()
	filledSubj := adminbot.TemplateExampleSubjectFilled()
	return "📝 <b>Новый шаблон</b>\n\nШаг 1/3: введите <b>название</b>.\n\n" +
		help.String() + "\n<b>Пример темы:</b>\n<pre>" + html.EscapeString(exSubj) + "</pre>\n" +
		"<b>Тема с подстановкой:</b>\n<pre>" + html.EscapeString(filledSubj) + "</pre>\n\n" +
		"<b>Пример текста:</b>\n<pre>" + html.EscapeString(ex) + "</pre>\n\n" +
		"<b>Текст с подстановкой:</b>\n<pre>" + html.EscapeString(filled) + "</pre>"
}

func renderWorkerTemplates(db *sql.DB, userID int64) (string, tgbotapi.InlineKeyboardMarkup) {
	tpls, err := listingsdb.ListEmailTemplates(db, userID)
	if err != nil || len(tpls) == 0 {
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("➕ Добавить", "worker_tpl_add")),
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "worker_main")),
		)
		return "📝 <b>Шаблоны сообщений</b>\n\nНет шаблонов.", kb
	}
	var lines []string
	lines = append(lines, "📝 <b>Шаблоны</b>")
	lines = append(lines, "<i>При рассылке по кругу: 1‑е письмо — 1‑й шаблон, 2‑е — 2‑й…</i>")
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, t := range tpls {
		prev := t.Body
		if utf8.RuneCountInString(prev) > 50 {
			prev = string([]rune(prev)[:50]) + "…"
		}
		lines = append(lines, fmt.Sprintf("• <b>%s</b>\n  <i>%s</i>", html.EscapeString(t.Name), html.EscapeString(prev)))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✏️ Правка", fmt.Sprintf("worker_tpl_edit_%d", t.ID)),
			tgbotapi.NewInlineKeyboardButtonData("🗑", fmt.Sprintf("worker_tpl_del_%d", t.ID)),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("➕ Добавить", "worker_tpl_add")))
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "worker_main")))
	text := strings.Join(lines, "\n")
	if len(text) > 4000 {
		text = text[:3997] + "..."
	}
	return text, tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func buildWorkerEmailsListPage(db *sql.DB, userID int64, page int) (string, tgbotapi.InlineKeyboardMarkup) {
	offset := page * emailsPerPage
	rows, err := listingsdb.ListEmails(db, userID, emailsPerPage, offset)
	if err != nil {
		rows = nil
	}
	total, _ := listingsdb.EmailsTotalCount(db, userID)
	activeN, _ := listingsdb.ActiveEmailsCount(db, userID)
	if len(rows) == 0 {
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ К меню почт", "worker_emails")),
		)
		return "📋 <b>Список почт</b>\n\nПусто.", kb
	}
	blockedCount := total - activeN
	var lines []string
	lines = append(lines, fmt.Sprintf("📋 <b>Почты</b> (стр. %d, всего %d, активных: %d)", page+1, total, activeN))
	if blockedCount > 0 {
		lines = append(lines, "⚠️ Заблокированные (🚫) не используются.\n")
	}
	var kbRows [][]tgbotapi.InlineKeyboardButton
	for idx, r := range rows {
		line := "• <code>" + html.EscapeString(r.Email) + "</code>"
		if r.Blocked {
			line += " 🚫 заблокирована"
		}
		lines = append(lines, line)
		var row []tgbotapi.InlineKeyboardButton
		if r.Blocked {
			row = append(row, tgbotapi.NewInlineKeyboardButtonData("✅ Разблокировать", fmt.Sprintf("worker_email_unblock_%d_%d", page, idx)))
		}
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("🗑 Удалить", fmt.Sprintf("worker_email_del_%d_%d", page, idx)))
		kbRows = append(kbRows, row)
	}
	var nav []tgbotapi.InlineKeyboardButton
	if page > 0 {
		nav = append(nav, tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", fmt.Sprintf("worker_emails_list_%d", page-1)))
	}
	if offset+len(rows) < total {
		nav = append(nav, tgbotapi.NewInlineKeyboardButtonData("Вперёд ▶️", fmt.Sprintf("worker_emails_list_%d", page+1)))
	}
	if len(nav) > 0 {
		kbRows = append(kbRows, nav)
	}
	kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ К меню почт", "worker_emails")))
	text := strings.Join(lines, "\n")
	if len(text) > 4000 {
		text = text[:3997] + "..."
	}
	return text, tgbotapi.InlineKeyboardMarkup{InlineKeyboard: kbRows}
}

var bulkDelayOptions = []struct {
	Sec   int
	Label string
}{
	{0, "Без задержки"},
	{1, "1 с"},
	{5, "5 с"},
	{10, "10 с"},
	{30, "30 с"},
	{60, "1 мин"},
	{120, "2 мин"},
	{180, "3 мин"},
	{300, "5 мин"},
	{600, "10 мин"},
	{900, "15 мин"},
	{1800, "30 мин"},
}

func workerBulkDelayKB() tgbotapi.InlineKeyboardMarkup {
	var rows [][]tgbotapi.InlineKeyboardButton
	for i := 0; i < len(bulkDelayOptions); i += 2 {
		var row []tgbotapi.InlineKeyboardButton
		for j := i; j < i+2 && j < len(bulkDelayOptions); j++ {
			o := bulkDelayOptions[j]
			row = append(row, tgbotapi.NewInlineKeyboardButtonData(o.Label, fmt.Sprintf("worker_bulk_delay_%d", o.Sec)))
		}
		rows = append(rows, row)
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "worker_main")))
	return tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
}
