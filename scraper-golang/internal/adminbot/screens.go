package adminbot

import (
	"database/sql"
	"fmt"
	"html"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
)

func mainPanelHTML() string {
	return "👑 <b>Панель администратора</b>\n\nВыберите действие:"
}

func (b *Bot) showPending(chatID int64, msgID int) {
	pending, err := listingsdb.PendingUsers(b.db)
	if err != nil {
		b.editHTML(chatID, msgID, "Ошибка БД: "+html.EscapeString(err.Error()), kbBackMain())
		return
	}
	if len(pending) == 0 {
		b.editHTML(chatID, msgID, "📋 <b>Ожидающие подтверждения</b>\n\nНет заявок.", kbBackMain())
		return
	}
	var lines []string
	lines = append(lines, "📋 <b>Ожидающие подтверждения</b>")
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, u := range pending {
		if len(rows) >= 15 {
			break
		}
		created := u.CreatedAt
		if len(created) > 10 {
			created = created[:10]
		}
		lines = append(lines, fmt.Sprintf("• ID <code>%d</code> — %s", u.UserID, html.EscapeString(created)))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("✅ Одобрить %d", u.UserID), fmt.Sprintf("approve_%d", u.UserID)),
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("❌ Отклонить %d", u.UserID), fmt.Sprintf("reject_%d", u.UserID)),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")))
	b.editHTML(chatID, msgID, strings.Join(lines, "\n"), tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows})
}

func (b *Bot) showWorkers(chatID int64, msgID int) {
	stats, err := listingsdb.WorkersWithStats(b.db)
	if err != nil {
		stats = nil
	}
	if len(stats) == 0 {
		ws, _ := listingsdb.AllWorkers(b.db)
		for _, w := range ws {
			stats = append(stats, listingsdb.WorkerStat{
				UserID: w.UserID, CreatedAt: w.CreatedAt, ShiftActive: w.ShiftActive,
				ListingsToday: 0, LastListingAt: "—",
			})
		}
	}
	if len(stats) == 0 {
		b.editHTML(chatID, msgID, "👥 <b>Воркеры</b>\n\nНет авторизованных воркеров.", kbBackMain())
		return
	}
	var lines []string
	lines = append(lines, "👥 <b>Воркеры</b>")
	var rows [][]tgbotapi.InlineKeyboardButton
	for i, w := range stats {
		if i >= 20 {
			break
		}
		shift := "⚪ не на смене"
		if w.ShiftActive {
			shift = "🟢 на смене"
		}
		cr := w.CreatedAt
		if len(cr) > 10 {
			cr = cr[:10]
		}
		lines = append(lines, fmt.Sprintf(
			"• ID <code>%d</code> — %s\n  📅 Рег: %s | 📦 Сегодня: %d | 🕐 Последний: %s",
			w.UserID, shift, html.EscapeString(cr), w.ListingsToday, html.EscapeString(w.LastListingAt)))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("🚫 Блок %d", w.UserID), fmt.Sprintf("block_%d", w.UserID)),
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("🗑 Удалить %d", w.UserID), fmt.Sprintf("delete_%d", w.UserID)),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")))
	b.editHTML(chatID, msgID, strings.Join(lines, "\n"), tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows})
}

func (b *Bot) showBlocked(chatID int64, msgID int) {
	list, err := listingsdb.BlockedUsersList(b.db)
	if err != nil {
		b.editHTML(chatID, msgID, "Ошибка: "+html.EscapeString(err.Error()), kbBackMain())
		return
	}
	if len(list) == 0 {
		b.editHTML(chatID, msgID, "🚫 <b>Заблокированные</b>\n\nНет заблокированных.", kbBackMain())
		return
	}
	var lines []string
	lines = append(lines, "🚫 <b>Заблокированные</b>")
	var rows [][]tgbotapi.InlineKeyboardButton
	for i, u := range list {
		if i >= 20 {
			break
		}
		ba := u.BlockedAt
		if len(ba) > 10 {
			ba = ba[:10]
		}
		lines = append(lines, fmt.Sprintf("• ID <code>%d</code> — %s", u.UserID, html.EscapeString(ba)))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("🔓 Разблокировать %d", u.UserID), fmt.Sprintf("unblock_%d", u.UserID)),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")))
	b.editHTML(chatID, msgID, strings.Join(lines, "\n"), tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows})
}

func emailsMenuKB(db *sql.DB, ownerID int64) tgbotapi.InlineKeyboardMarkup {
	n, _ := listingsdb.EmailsTotalCount(db, ownerID)
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("➕ Добавить (mail:apppassword)", "emails_add")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📤 Загрузить CSV", "emails_upload")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("📋 Список (%d)", n), "emails_list_0")),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📧 Тест почты", "emails_test"),
			tgbotapi.NewInlineKeyboardButtonData("🔄 Протестировать все", "emails_test_all"),
		),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📥 Экспорт CSV", "emails_export")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")),
	)
}

func (b *Bot) showEmailsMenu(chatID int64, msgID int) {
	n, _ := listingsdb.EmailsTotalCount(b.db, b.ownerID)
	text := fmt.Sprintf(
		"📧 <b>База почт</b>\n\nВсего: %d\n\n"+
			"• Добавить — mail:apppassword (несколько строк)\n"+
			"• Загрузить CSV — файл .csv\n"+
			"• Список — просмотр и удаление", n)
	b.editHTML(chatID, msgID, text, emailsMenuKB(b.db, b.ownerID))
}

func emailsAddHTML() string {
	return "➕ <b>Добавить почты</b>\n\nТолько Gmail. Формат:\n<code>mail@gmail.com:apppassword</code>\n\n" +
		"Несколько строк — через Enter. Разделители: <code>:</code> <code>;</code> <code>Tab</code>"
}

func emailsUploadHTML() string {
	return "📤 <b>Загрузить CSV</b>\n\nКолонки: email, apppassword\n(или почта / пароль)"
}

func (b *Bot) showEmailsList(chatID int64, msgID int, page int) {
	offset := page * emailsPerPage
	rows, err := listingsdb.ListEmails(b.db, b.ownerID, emailsPerPage, offset)
	if err != nil {
		b.editHTML(chatID, msgID, "Ошибка: "+html.EscapeString(err.Error()), emailsMenuKB(b.db, b.ownerID))
		return
	}
	total, _ := listingsdb.EmailsTotalCount(b.db, b.ownerID)
	lastUsed := listingsdb.LastUsedEmail(b.db, b.ownerID)
	if len(rows) == 0 {
		b.editHTML(chatID, msgID, "📋 <b>Список почт</b>\n\nПусто.", tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ К меню почт", "admin_emails")),
		))
		return
	}
	var lines []string
	lines = append(lines, fmt.Sprintf("📋 <b>Почты</b> (стр. %d, всего %d)", page+1, total))
	var kbRows [][]tgbotapi.InlineKeyboardButton
	for _, r := range rows {
		badge := ""
		if strings.EqualFold(r.Email, lastUsed) && !r.Blocked {
			badge = " ✉️ активна"
		} else if r.Blocked {
			badge = " 🚫"
		}
		lines = append(lines, fmt.Sprintf("• <code>%s</code>%s", html.EscapeString(r.Email), badge))
		safe := emailToCallbackSafe(r.Email)
		var row []tgbotapi.InlineKeyboardButton
		if r.Blocked {
			row = append(row, tgbotapi.NewInlineKeyboardButtonData("↩️ Разблокировать", fmt.Sprintf("emails_unblock_%d_%s", page, safe)))
		}
		mask := r.Email
		if at := strings.Index(mask, "@"); at > 0 && at < len(mask) {
			mask = mask[:3] + "***" + mask[at:]
		}
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("🗑 "+mask, fmt.Sprintf("emails_del_%d_%s", page, safe)))
		kbRows = append(kbRows, row)
	}
	var nav []tgbotapi.InlineKeyboardButton
	if page > 0 {
		nav = append(nav, tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", fmt.Sprintf("emails_list_%d", page-1)))
	}
	if offset+len(rows) < total {
		nav = append(nav, tgbotapi.NewInlineKeyboardButtonData("Вперёд ▶️", fmt.Sprintf("emails_list_%d", page+1)))
	}
	if len(nav) > 0 {
		kbRows = append(kbRows, nav)
	}
	kbRows = append(kbRows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ К меню почт", "admin_emails")))
	text := strings.Join(lines, "\n")
	if len(text) > 4000 {
		text = text[:3997] + "..."
	}
	b.editHTML(chatID, msgID, text, tgbotapi.InlineKeyboardMarkup{InlineKeyboard: kbRows})
}

func templatesListHTML(db *sql.DB, ownerID int64) string {
	tpls, err := listingsdb.ListEmailTemplates(db, ownerID)
	if err != nil || len(tpls) == 0 {
		return "📝 <b>Шаблоны сообщений</b>\n\nНет шаблонов."
	}
	activeID, hasActive := listingsdb.ActiveTemplateID(db, ownerID)
	var lines []string
	lines = append(lines, "📝 <b>Шаблоны</b>")
	for _, t := range tpls {
		prev := t.Body
		if len([]rune(prev)) > 50 {
			prev = string([]rune(prev)[:50]) + "…"
		}
		badge := ""
		if hasActive && t.ID == activeID {
			badge = " ✅ активен"
		}
		lines = append(lines, fmt.Sprintf("• <b>%s</b>%s\n  <i>%s</i>", html.EscapeString(t.Name), badge, html.EscapeString(prev)))
	}
	out := strings.Join(lines, "\n")
	if len(out) > 4000 {
		return out[:3997] + "..."
	}
	return out
}

func templatesListKB(db *sql.DB, ownerID int64) tgbotapi.InlineKeyboardMarkup {
	tpls, _ := listingsdb.ListEmailTemplates(db, ownerID)
	activeID, hasActive := listingsdb.ActiveTemplateID(db, ownerID)
	var rows [][]tgbotapi.InlineKeyboardButton
	for _, t := range tpls {
		label := "▶️ Выбрать"
		if hasActive && t.ID == activeID {
			label = "✓ Активен"
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(label, fmt.Sprintf("tpl_activate_%d", t.ID)),
			tgbotapi.NewInlineKeyboardButtonData("✏️", fmt.Sprintf("tpl_edit_%d", t.ID)),
			tgbotapi.NewInlineKeyboardButtonData("🗑", fmt.Sprintf("tpl_del_%d", t.ID)),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("➕ Добавить", "tpl_add")))
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")))
	return tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (b *Bot) showTemplates(chatID int64, msgID int) {
	tpls, _ := listingsdb.ListEmailTemplates(b.db, b.ownerID)
	if len(tpls) == 0 {
		kb := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("➕ Добавить", "tpl_add")),
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")),
		)
		b.editHTML(chatID, msgID, "📝 <b>Шаблоны сообщений</b>\n\nНет шаблонов.", kb)
		return
	}
	b.editHTML(chatID, msgID, templatesListHTML(b.db, b.ownerID), templatesListKB(b.db, b.ownerID))
}

func tplAddStep1HTML() string {
	var help strings.Builder
	help.WriteString("<b>Доступные переменные:</b>\n")
	for k, desc := range TemplateVarDescriptions {
		help.WriteString(fmt.Sprintf("• <code>{%s}</code> — %s\n", k, html.EscapeString(desc)))
	}
	ex := templateExampleBody()
	filled := FormatTemplateExampleBody(ex)
	return "📝 <b>Новый шаблон</b>\n\nШаг 1/2: введите <b>название</b> шаблона.\n\n" +
		help.String() + "\n<b>Пример шаблона:</b>\n<pre>" + html.EscapeString(ex) + "</pre>\n\n" +
		"<b>Пример с подстановкой:</b>\n<pre>" + html.EscapeString(filled) + "</pre>"
}
