package clientbot

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/adminbot"
	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/mailer"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

func (b *Bot) answerCallback(cbID, text string) {
	_, err := b.api.Request(tgbotapi.NewCallback(cbID, text))
	if err != nil {
		prettylog.Warnf("клиент-бот answerCallbackQuery · %v", err)
	} else if strings.TrimSpace(text) != "" {
		prettylog.Workerf("· всплывашка · %s", previewRunes(text, 72))
	}
}

func (b *Bot) handleUpdate(u tgbotapi.Update) {
	switch {
	case u.CallbackQuery != nil:
		prettylog.Workerf("апдейт · кнопка · id=%s", u.CallbackQuery.ID)
		b.onCallback(u.CallbackQuery)
	case u.Message != nil:
		m := u.Message
		kind := "текст"
		if m.Document != nil {
			kind = fmt.Sprintf("документ %q", m.Document.FileName)
		} else if len(m.Photo) > 0 {
			kind = "фото"
		}
		uid := int64(0)
		if m.From != nil {
			uid = m.From.ID
		}
		prettylog.Workerf("апдейт · сообщение · %s · user=%d chat=%d", kind, uid, m.Chat.ID)
		b.onMessage(m)
	default:
		prettylog.Worker("апдейт · пустой", "")
	}
}

func (b *Bot) onCallback(cb *tgbotapi.CallbackQuery) {
	if cb.Message == nil {
		b.answerCallback(cb.ID, "")
		return
	}
	data := cb.Data
	chatID := cb.Message.Chat.ID
	msgID := cb.Message.MessageID
	fromID := int64(0)
	if cb.From != nil {
		fromID = cb.From.ID
	}

	// Админ жмёт одобрить/отклонить в своём чате (как в Python — тот же клиент-бот может доставлять уведомление).
	if strings.HasPrefix(data, "approve_") && chatID == b.cfg.AdminChatID {
		b.handleAdminApprove(cb, true)
		return
	}
	if strings.HasPrefix(data, "reject_") && chatID == b.cfg.AdminChatID {
		b.handleAdminApprove(cb, false)
		return
	}

	if listingsdb.IsBlocked(b.db, fromID) {
		prettylog.Workerf("кнопка игнор · user=%d заблокирован · %q", fromID, data)
		b.answerCallback(cb.ID, "")
		return
	}

	prettylog.Workerf("кнопка · user=%d chat=%d · %q", fromID, chatID, data)

	switch {
	case data == "shift_start":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "🔒 Сначала дождитесь одобрения")
			return
		}
		_ = listingsdb.SetShiftActive(b.db, fromID, true)
		prettylog.OKf("смена СТАРТ · user=%d", fromID)
		b.editMsgHTML(chatID, msgID,
			"🟢 <b>Смена начата</b>\n\nВы будете получать уведомления о новых товарах (&lt; 3 ч).",
			workerKB(true))
		b.answerCallback(cb.ID, "🟢 Смена начата")
	case data == "shift_stop":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		_ = listingsdb.SetShiftActive(b.db, fromID, false)
		prettylog.OKf("смена СТОП · user=%d", fromID)
		b.editMsgHTML(chatID, msgID,
			"⚪ <b>Смена закрыта</b>\n\nУведомления приостановлены.",
			workerKB(false))
		b.answerCallback(cb.ID, "⚪ Смена закрыта")
	case data == "list_today":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		items, err := listingsdb.WorkerListingsToday(b.db, fromID)
		if err != nil || len(items) == 0 {
			b.answerCallback(cb.ID, "📦 Сегодня товаров нет")
			return
		}
		var lines []string
		lines = append(lines, fmt.Sprintf("📦 <b>Товары сегодня (%d)</b>", len(items)))
		for i, it := range items {
			if i >= 25 {
				break
			}
			price := "—"
			if it.PriceCents.Valid {
				price = fmt.Sprintf("€%.2f", float64(it.PriceCents.Int64)/100)
			}
			title := it.Title
			if title == "" {
				title = "?"
			}
			r := []rune(title)
			if len(r) > 50 {
				title = string(r[:50])
			}
			lines = append(lines, fmt.Sprintf("%d. %s — %s", i+1, html.EscapeString(title), price))
			if it.ListingURL != "" {
				lines = append(lines, "   "+html.EscapeString(it.ListingURL))
			}
		}
		text := strings.Join(lines, "\n")
		if len(text) > 4000 {
			text = text[:3997] + "..."
		}
		prettylog.Workerf("товары сегодня · user=%d · строк %d", fromID, len(items))
		m := tgbotapi.NewMessage(chatID, text)
		m.ParseMode = "HTML"
		_, _ = b.api.Send(m)
		b.answerCallback(cb.ID, "")
	case data == "worker_emails":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		b.clearDialog()
		n, _ := listingsdb.EmailsTotalCount(b.db, fromID)
		b.editMsgHTML(chatID, msgID,
			fmt.Sprintf("📧 <b>База почт</b>\n\nВсего: %d\n\n• Добавить — несколько строк\n• CSV — файл .csv\n• Список", n),
			workerEmailsKB(b.db, fromID))
		b.answerCallback(cb.ID, "")
	case data == "worker_emails_add":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		b.dlgMu.Lock()
		b.dlgStep = "worker_emails"
		b.dlgMu.Unlock()
		prettylog.Workerf("диалог · ввод почт · user=%d", fromID)
		b.editMsgHTML(chatID, msgID, workerEmailsAddHTML(),
			tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "worker_emails"),
			)))
		b.answerCallback(cb.ID, "")
	case data == "worker_emails_upload":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		b.clearDialog()
		b.editMsgHTML(chatID, msgID, workerEmailsUploadHTML(),
			tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("◀️ К меню почт", "worker_emails"),
			)))
		b.answerCallback(cb.ID, "")
	case strings.HasPrefix(data, "worker_emails_list_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "worker_emails_list_"))
		txt, kb := buildWorkerEmailsListPage(b.db, fromID, page)
		b.editMsgHTML(chatID, msgID, txt, kb)
		b.answerCallback(cb.ID, "")
	case strings.HasPrefix(data, "worker_email_del_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		parts := strings.Split(strings.TrimPrefix(data, "worker_email_del_"), "_")
		if len(parts) < 2 {
			b.answerCallback(cb.ID, "Ошибка")
			return
		}
		page, _ := strconv.Atoi(parts[0])
		idx, _ := strconv.Atoi(parts[1])
		offset := page * emailsPerPage
		rows, _ := listingsdb.ListEmails(b.db, fromID, emailsPerPage, offset)
		if idx < 0 || idx >= len(rows) {
			b.answerCallback(cb.ID, "Почта не найдена")
			return
		}
		em := rows[idx].Email
		if ok, _ := listingsdb.DeleteEmail(b.db, fromID, em); ok {
			prettylog.Warnf("почта удалена · user=%d · %s", fromID, em)
			b.answerCallback(cb.ID, "✅ Удалена")
			total, _ := listingsdb.EmailsTotalCount(b.db, fromID)
			newPage := page
			if total > 0 {
				maxP := (total - 1) / emailsPerPage
				if newPage > maxP {
					newPage = maxP
				}
			} else {
				newPage = 0
			}
			txt, kb := buildWorkerEmailsListPage(b.db, fromID, newPage)
			b.editMsgHTML(chatID, msgID, txt, kb)
		} else {
			b.answerCallback(cb.ID, "Не удалось удалить")
		}
	case strings.HasPrefix(data, "worker_email_unblock_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		parts := strings.Split(strings.TrimPrefix(data, "worker_email_unblock_"), "_")
		if len(parts) < 2 {
			b.answerCallback(cb.ID, "Ошибка")
			return
		}
		page, _ := strconv.Atoi(parts[0])
		idx, _ := strconv.Atoi(parts[1])
		offset := page * emailsPerPage
		rows, _ := listingsdb.ListEmails(b.db, fromID, emailsPerPage, offset)
		if idx < 0 || idx >= len(rows) {
			b.answerCallback(cb.ID, "Почта не найдена")
			return
		}
		em := rows[idx].Email
		if ok, _ := listingsdb.UnblockEmail(b.db, fromID, em); ok {
			prettylog.OKf("почта разблокирована · user=%d · %s", fromID, em)
			b.answerCallback(cb.ID, "✅ Разблокирована")
			txt, kb := buildWorkerEmailsListPage(b.db, fromID, page)
			b.editMsgHTML(chatID, msgID, txt, kb)
		} else {
			b.answerCallback(cb.ID, "Не удалось разблокировать")
		}
	case data == "worker_main":
		b.clearDialog()
		if listingsdb.IsBlocked(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		if listingsdb.IsAuthorized(b.db, fromID) {
			on := listingsdb.IsShiftActive(b.db, fromID)
			b.editMsgHTML(chatID, msgID,
				"👋 Добро пожаловать!\n\nНачните смену, чтобы получать уведомления о новых товарах (&lt; 3 ч).",
				workerKB(on))
		} else {
			b.editMsgHTML(chatID, msgID, pendingRegText(), tgbotapi.InlineKeyboardMarkup{InlineKeyboard: [][]tgbotapi.InlineKeyboardButton{}})
		}
		b.answerCallback(cb.ID, "")
	case data == "worker_templates":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		b.clearDialog()
		txt, kb := renderWorkerTemplates(b.db, fromID)
		b.editMsgHTML(chatID, msgID, txt, kb)
		b.answerCallback(cb.ID, "")
	case data == "worker_tpl_add":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		b.dlgMu.Lock()
		b.dlgStep = "worker_tpl_name"
		b.dlgTplEditID = 0
		b.dlgTplName = ""
		b.dlgMu.Unlock()
		b.editMsgHTML(chatID, msgID, workerTplAddHTML(),
			tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "worker_templates"),
			)))
		b.answerCallback(cb.ID, "")
	case strings.HasPrefix(data, "worker_tpl_activate_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "worker_tpl_activate_"), 10, 64)
		if _, _, ok := listingsdb.GetEmailTemplate(b.db, fromID, tid); !ok {
			b.answerCallback(cb.ID, "Шаблон не найден")
			return
		}
		_ = listingsdb.SetActiveTemplateID(b.db, fromID, tid)
		prettylog.OKf("активный шаблон · user=%d · id=%d", fromID, tid)
		txt, kb := renderWorkerTemplates(b.db, fromID)
		b.editMsgHTML(chatID, msgID, txt, kb)
		b.answerCallback(cb.ID, "✅ Шаблон активирован")
	case strings.HasPrefix(data, "worker_tpl_edit_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "worker_tpl_edit_"), 10, 64)
		name, body, ok := listingsdb.GetEmailTemplate(b.db, fromID, tid)
		if !ok {
			b.answerCallback(cb.ID, "Не найден")
			return
		}
		b.dlgMu.Lock()
		b.dlgStep = "worker_tpl_body"
		b.dlgTplEditID = tid
		b.dlgTplName = name
		b.dlgMu.Unlock()
		b.editMsgHTML(chatID, msgID,
			fmt.Sprintf("✏️ Редактирование «%s»\n\nОтправьте новый текст шаблона:", html.EscapeString(name)),
			tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "worker_templates"),
			)))
		b.sendHTML(chatID, "<pre>"+html.EscapeString(body)+"</pre>")
		b.answerCallback(cb.ID, "")
	case strings.HasPrefix(data, "worker_tpl_del_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "worker_tpl_del_"), 10, 64)
		_ = listingsdb.ClearActiveTemplateIf(b.db, fromID, tid)
		if ok, _ := listingsdb.DeleteEmailTemplate(b.db, fromID, tid); ok {
			prettylog.Warnf("шаблон удалён · user=%d · id=%d", fromID, tid)
			txt, kb := renderWorkerTemplates(b.db, fromID)
			b.editMsgHTML(chatID, msgID, txt, kb)
			b.answerCallback(cb.ID, "🗑 Удалён")
		} else {
			b.answerCallback(cb.ID, "Не найден")
		}
	case data == "worker_bulk_mail":
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		n, _ := listingsdb.EmailsTotalCount(b.db, fromID)
		if n == 0 {
			b.answerCallback(cb.ID, "❌ Сначала добавьте почты")
			return
		}
		if _, ok := listingsdb.ActiveTemplateID(b.db, fromID); !ok {
			b.answerCallback(cb.ID, "❌ Выберите активный шаблон")
			return
		}
		b.editMsgHTML(chatID, msgID,
			"📤 <b>Рассылка по CSV</b>\n\nВыберите задержку между письмами:",
			workerBulkDelayKB())
		b.answerCallback(cb.ID, "")
	case strings.HasPrefix(data, "worker_bulk_delay_"):
		if !listingsdb.IsAuthorized(b.db, fromID) {
			b.answerCallback(cb.ID, "")
			return
		}
		sec, _ := strconv.Atoi(strings.TrimPrefix(data, "worker_bulk_delay_"))
		b.dlgMu.Lock()
		b.dlgStep = "worker_bulk_csv"
		b.dlgBulkDelay = time.Duration(sec) * time.Second
		b.dlgMu.Unlock()
		label := "Без задержки"
		for _, o := range bulkDelayOptions {
			if o.Sec == sec {
				label = o.Label
				break
			}
		}
		prettylog.Workerf("рассылка · выбрана задержка %s · user=%d", label, fromID)
		b.editMsgHTML(chatID, msgID,
			fmt.Sprintf("📤 <b>Рассылка по CSV</b> · задержка: <b>%s</b>\n\nЗагрузите .csv (продавец + ссылка marktplaats/2dehands).", html.EscapeString(label)),
			tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "worker_main"),
			)))
		b.answerCallback(cb.ID, "")
	default:
		prettylog.Workerf("кнопка без обработчика · %q", data)
		b.answerCallback(cb.ID, "")
	}
}

func (b *Bot) handleAdminApprove(cb *tgbotapi.CallbackQuery, approve bool) {
	var uidStr string
	if approve {
		uidStr = strings.TrimPrefix(cb.Data, "approve_")
	} else {
		uidStr = strings.TrimPrefix(cb.Data, "reject_")
	}
	workerID, err := strconv.ParseInt(uidStr, 10, 64)
	if err != nil {
		b.answerCallback(cb.ID, "Ошибка id")
		return
	}
	chatID := cb.Message.Chat.ID
	msgID := cb.Message.MessageID
	old := cb.Message.Text
	if approve {
		_ = listingsdb.AuthorizeUser(b.db, workerID)
		prettylog.OKf("админ одобрил воркера · user_id=%d", workerID)
		txt := "✅ <b>Ваша заявка одобрена!</b>\n\nТеперь вы можете пользоваться ботом. Нажмите /start для начала."
		m := tgbotapi.NewMessage(workerID, txt)
		m.ParseMode = "HTML"
		if _, err := b.api.Send(m); err != nil {
			prettylog.Warnf("уведомление воркеру %d · %v", workerID, err)
		}
		newText := old + "\n\n✅ Одобрено"
		b.editMsgHTML(chatID, msgID, newText, tgbotapi.InlineKeyboardMarkup{InlineKeyboard: [][]tgbotapi.InlineKeyboardButton{}})
		b.answerCallback(cb.ID, "✅ Воркер одобрен")
	} else {
		_ = listingsdb.BlockUser(b.db, workerID)
		prettylog.Warnf("админ отклонил (блок) · user_id=%d", workerID)
		newText := old + "\n\n❌ Отклонён и заблокирован"
		b.editMsgHTML(chatID, msgID, newText, tgbotapi.InlineKeyboardMarkup{InlineKeyboard: [][]tgbotapi.InlineKeyboardButton{}})
		b.answerCallback(cb.ID, "❌ Отклонён")
	}
}

func (b *Bot) notifyAdminNewWorker(workerID int64, u *tgbotapi.User) {
	if b.cfg.AdminChatID == 0 {
		prettylog.Warn("новый воркер · ADMIN_CHAT_ID не задан — админ не уведомлён", "")
		return
	}
	name := ""
	if u != nil {
		name = strings.TrimSpace(u.FirstName + " " + u.LastName)
	}
	uname := "—"
	if u != nil && u.UserName != "" {
		uname = "@" + u.UserName
	}
	text := fmt.Sprintf(
		"📩 <b>Новый воркер</b>\n\n👤 %s\n🆔 ID: <code>%d</code>\n📱 %s",
		html.EscapeString(name), workerID, html.EscapeString(uname),
	)
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Одобрить", fmt.Sprintf("approve_%d", workerID)),
			tgbotapi.NewInlineKeyboardButtonData("❌ Отклонить", fmt.Sprintf("reject_%d", workerID)),
		),
	)
	m := tgbotapi.NewMessage(b.cfg.AdminChatID, text)
	m.ParseMode = "HTML"
	m.ReplyMarkup = kb
	api := b.notifyAPI()
	prettylog.Workerf("→ уведомление админу · chat=%d · worker=%d · бот=%v", b.cfg.AdminChatID, workerID, api == b.adminAPI)
	if _, err := api.Send(m); err != nil {
		prettylog.Warnf("уведомление админу · %v", err)
	}
}

func (b *Bot) onMessage(msg *tgbotapi.Message) {
	uid := int64(0)
	if msg.From != nil {
		uid = msg.From.ID
	}
	if listingsdb.IsBlocked(b.db, uid) {
		prettylog.Workerf("игнор · user=%d в blocked_users", uid)
		return
	}

	b.dlgMu.Lock()
	step := b.dlgStep
	bulkDelay := b.dlgBulkDelay
	b.dlgMu.Unlock()

	if msg.Document != nil && msg.Document.FileName != "" &&
		strings.HasSuffix(strings.ToLower(msg.Document.FileName), ".csv") {
		if step == "worker_bulk_csv" && listingsdb.IsAuthorized(b.db, uid) {
			b.handleBulkMailCSV(msg, bulkDelay)
			return
		}
		if listingsdb.IsAuthorized(b.db, uid) {
			b.handleWorkerEmailsCSV(msg)
			return
		}
		prettylog.Workerf("CSV игнор · user=%d не авторизован и не в режиме рассылки", uid)
		return
	}

	if msg.Text == "" {
		return
	}

	prettylog.Workerf("текст · user=%d · шаг=%q · %s", uid, step, previewOneLine(msg.Text, 160))

	if msg.IsCommand() && msg.Command() == "start" {
		if listingsdb.IsAuthorized(b.db, uid) {
			on := listingsdb.IsShiftActive(b.db, uid)
			prettylog.OKf("/start · авторизован · user=%d · смена=%v", uid, on)
			m := tgbotapi.NewMessage(msg.Chat.ID,
				"👋 Добро пожаловать!\n\nНачните смену, чтобы получать уведомления о новых товарах (&lt; 3 ч).")
			m.ParseMode = "HTML"
			m.ReplyMarkup = workerKB(on)
			_, _ = b.api.Send(m)
			return
		}
		_ = listingsdb.RegisterPendingUser(b.db, uid)
		prettylog.Workerf("регистрация · pending · user=%d", uid)
		b.notifyAdminNewWorker(uid, msg.From)
		m := tgbotapi.NewMessage(msg.Chat.ID, pendingRegText())
		m.ParseMode = "HTML"
		_, _ = b.api.Send(m)
		return
	}

	switch step {
	case "worker_emails":
		if !listingsdb.IsAuthorized(b.db, uid) {
			return
		}
		pairs := adminbot.ParseEmailsText(msg.Text)
		if len(pairs) == 0 {
			b.sendPlain(msg.Chat.ID, "❌ Не найдено валидных строк. Формат: mail@gmail.com:apppassword")
			return
		}
		added, skipped := listingsdb.AddEmailsBatch(b.db, uid, pairs)
		total, _ := listingsdb.EmailsTotalCount(b.db, uid)
		active, _ := listingsdb.ActiveEmailsCount(b.db, uid)
		prettylog.OKf("почты текстом · user=%d · +%d пропуск %d · всего %d активн %d", uid, added, skipped, total, active)
		b.clearDialog()
		b.sendPlain(msg.Chat.ID, fmt.Sprintf("✅ Добавлено: %d, пропущено (дубли): %d\n📋 Всего: %d, активных: %d", added, skipped, total, active))
		m := tgbotapi.NewMessage(msg.Chat.ID, "📧 База почт")
		m.ReplyMarkup = workerEmailsKB(b.db, uid)
		_, _ = b.api.Send(m)
	case "worker_tpl_name":
		if !listingsdb.IsAuthorized(b.db, uid) {
			return
		}
		name := strings.TrimSpace(msg.Text)
		if name == "" {
			b.sendPlain(msg.Chat.ID, "Введите название")
			return
		}
		b.dlgMu.Lock()
		b.dlgTplName = name
		b.dlgStep = "worker_tpl_body"
		b.dlgMu.Unlock()
		var vars strings.Builder
		for k := range adminbot.TemplateVarDescriptions {
			if vars.Len() > 0 {
				vars.WriteString(", ")
			}
			vars.WriteString("<code>{")
			vars.WriteString(k)
			vars.WriteString("}</code>")
		}
		b.sendHTML(msg.Chat.ID, "Шаг 2/2: введите <b>текст шаблона</b>.\n\nПеременные: "+vars.String())
	case "worker_tpl_body":
		if !listingsdb.IsAuthorized(b.db, uid) {
			return
		}
		body := msg.Text
		b.dlgMu.Lock()
		name := b.dlgTplName
		eid := b.dlgTplEditID
		b.dlgMu.Unlock()
		if eid > 0 {
			if _, _, ok := listingsdb.GetEmailTemplate(b.db, uid, eid); !ok {
				b.clearDialog()
				return
			}
			_, _ = listingsdb.UpdateEmailTemplate(b.db, uid, eid, name, body)
			prettylog.OKf("шаблон обновлён · user=%d · id=%d", uid, eid)
			b.sendPlain(msg.Chat.ID, fmt.Sprintf("✅ Шаблон «%s» обновлён", name))
		} else {
			_, _ = listingsdb.AddEmailTemplate(b.db, uid, name, body)
			prettylog.OKf("шаблон создан · user=%d · %q", uid, name)
			b.sendPlain(msg.Chat.ID, fmt.Sprintf("✅ Шаблон «%s» добавлен", name))
		}
		b.clearDialog()
		txt, kb := renderWorkerTemplates(b.db, uid)
		m := tgbotapi.NewMessage(msg.Chat.ID, txt)
		m.ParseMode = "HTML"
		m.ReplyMarkup = kb
		_, _ = b.api.Send(m)
	default:
		if listingsdb.IsAuthorized(b.db, uid) {
			on := listingsdb.IsShiftActive(b.db, uid)
			m := tgbotapi.NewMessage(msg.Chat.ID, "Выберите действие:")
			m.ReplyMarkup = workerKB(on)
			_, _ = b.api.Send(m)
		} else {
			m := tgbotapi.NewMessage(msg.Chat.ID, pendingRegText())
			m.ParseMode = "HTML"
			_, _ = b.api.Send(m)
		}
	}
}

// downloadByHTTP — тот же клиент, что у Telegram API (в т.ч. прокси), для URL из file.Link().
func (b *Bot) downloadByHTTP(urlStr string) ([]byte, error) {
	if b.httpClient == nil {
		b.httpClient = &http.Client{Timeout: 120 * time.Second}
	}
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return io.ReadAll(resp.Body)
}

func (b *Bot) handleWorkerEmailsCSV(msg *tgbotapi.Message) {
	uid := msg.From.ID
	file, err := b.api.GetFile(tgbotapi.FileConfig{FileID: msg.Document.FileID})
	if err != nil {
		prettylog.Warnf("CSV почты · getFile · %v", err)
		b.sendPlain(msg.Chat.ID, "❌ Файл: "+err.Error())
		return
	}
	urlStr := file.Link(b.api.Token)
	data, err := b.downloadByHTTP(urlStr)
	if err != nil {
		prettylog.Warnf("CSV почты · скачивание · %v", err)
		b.sendPlain(msg.Chat.ID, "❌ Скачивание (нужен тот же прокси, что для Telegram): "+err.Error())
		return
	}
	pairs := adminbot.ParseEmailsCSV(string(data))
	if len(pairs) == 0 {
		b.sendPlain(msg.Chat.ID, "❌ В CSV не найдено email")
		return
	}
	added, skipped := listingsdb.AddEmailsBatch(b.db, uid, pairs)
	total, _ := listingsdb.EmailsTotalCount(b.db, uid)
	active, _ := listingsdb.ActiveEmailsCount(b.db, uid)
	prettylog.OKf("CSV почты · user=%d · +%d · всего %d активн %d", uid, added, total, active)
	b.clearDialog()
	b.sendPlain(msg.Chat.ID, fmt.Sprintf("✅ Из CSV: добавлено %d, пропущено %d\n📋 Всего: %d, активных: %d", added, skipped, total, active))
	m := tgbotapi.NewMessage(msg.Chat.ID, "📧 База почт")
	m.ReplyMarkup = workerEmailsKB(b.db, uid)
	_, _ = b.api.Send(m)
}

func (b *Bot) handleBulkMailCSV(msg *tgbotapi.Message, delay time.Duration) {
	uid := msg.From.ID
	chatID := msg.Chat.ID

	activeBefore, _ := listingsdb.ActiveEmailsCount(b.db, uid)
	if _, ok := listingsdb.ActiveTemplateID(b.db, uid); !ok || activeBefore == 0 {
		b.clearDialog()
		if activeBefore == 0 {
			b.sendHTML(chatID, "❌ <b>Нет активных почт</b>")
		} else {
			b.sendHTML(chatID, "❌ <b>Нет активного шаблона</b>")
		}
		return
	}

	file, err := b.api.GetFile(tgbotapi.FileConfig{FileID: msg.Document.FileID})
	if err != nil {
		b.clearDialog()
		b.sendPlain(chatID, "❌ Файл: "+err.Error())
		return
	}
	urlStr := file.Link(b.api.Token)
	raw, err := b.downloadByHTTP(urlStr)
	if err != nil {
		b.clearDialog()
		prettylog.Warnf("рассылка CSV · скачивание · %v", err)
		b.sendPlain(chatID, "❌ Скачивание файла (используется тот же прокси, что для Telegram): "+err.Error())
		return
	}

	listings := ParseListingsCSV(string(raw))
	if len(listings) == 0 {
		b.clearDialog()
		b.sendHTML(chatID, "❌ Не удалось распарсить CSV. Нужны колонки: продавец + ссылка на объявление (marktplaats, 2dehands, poshmark).")
		return
	}

	prettylog.OKf("рассылка CSV · принято · user=%d · объявлений=%d · файл=%q", uid, len(listings), msg.Document.FileName)
	confirmHTML := fmt.Sprintf("✅ <b>Файл принят</b>: %d объявлений.\n\n⏳ Отправляю письма…", len(listings))
	confirmMsg := tgbotapi.NewMessage(chatID, confirmHTML)
	confirmMsg.ParseMode = "HTML"
	status, err := b.api.Send(confirmMsg)
	if err != nil {
		prettylog.Warnf("рассылка · подтверждение в чат · %v", err)
		b.sendPlain(chatID, fmt.Sprintf("✅ Файл принят, %d объявлений. Запускаю рассылку… (ошибка HTML-сообщения: %v)", len(listings), err))
		status, err = b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("⏳ Рассылка %d писем…", len(listings))))
		if err != nil {
			prettylog.Warnf("рассылка · второе сообщение · %v", err)
			b.clearDialog()
			b.sendPlain(chatID, "⚠️ Не удалось отправить сообщение в Telegram. Рассылка всё равно запущена в фоне. Проверьте прокси.")
			status = tgbotapi.Message{MessageID: 0}
		}
	}
	statusID := status.MessageID
	b.clearDialog()

	go func() {
		skipRCPT := mailer.DevEnvironment()
		st := mailer.BulkSendListings(b.db, uid, listings, delay, skipRCPT)
		var sb strings.Builder
		sb.WriteString("✅ <b>Рассылка завершена</b>\n\n")
		sb.WriteString(fmt.Sprintf("📧 Отправлено: %d\n❌ Ошибок: %d\n👻 Нет почты продавца: %d\n📋 Строк: %d",
			st.OK, st.Fail, st.NotExists, len(listings)))
		if st.Fail+st.NotExists == len(listings) && st.OK == 0 {
			activeAfter, _ := listingsdb.ActiveEmailsCount(b.db, uid)
			if activeAfter == 0 && activeBefore > 0 {
				sb.WriteString("\n\n⚠️ <b>Почты заблокированы</b> после ошибок SMTP.")
			}
		}
		if statusID != 0 {
			edit := tgbotapi.NewEditMessageText(chatID, statusID, sb.String())
			edit.ParseMode = "HTML"
			if _, err := b.api.Send(edit); err != nil {
				prettylog.Warnf("edit статуса рассылки · %v", err)
				b.sendHTML(chatID, sb.String())
			}
		} else {
			b.sendHTML(chatID, sb.String())
		}
		on := listingsdb.IsShiftActive(b.db, uid)
		m2 := tgbotapi.NewMessage(chatID, "Главное меню")
		m2.ReplyMarkup = workerKB(on)
		if _, err := b.api.Send(m2); err != nil {
			prettylog.Warnf("меню после рассылки · %v", err)
		}
		prettylog.OKf("рассылка завершена · user=%d · ok=%d fail=%d", uid, st.OK, st.Fail)
	}()
}

func (b *Bot) editMsgHTML(chatID int64, msgID int, text string, kb tgbotapi.InlineKeyboardMarkup) {
	prettylog.Workerf("→ editMessage HTML · chat=%d msg=%d · %s", chatID, msgID, previewOneLine(text, 90))
	edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, msgID, text, kb)
	edit.ParseMode = "HTML"
	if _, err := b.api.Send(edit); err != nil {
		prettylog.Warnf("editMessage · %v", err)
	}
}

func (b *Bot) editMsgPlain(chatID int64, msgID int, text string, kb tgbotapi.InlineKeyboardMarkup) {
	edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, msgID, text, kb)
	if _, err := b.api.Send(edit); err != nil {
		prettylog.Warnf("editMessage · %v", err)
	}
}

func (b *Bot) sendHTML(chatID int64, text string) {
	prettylog.Workerf("→ sendMessage HTML · chat=%d · %s", chatID, previewOneLine(text, 120))
	m := tgbotapi.NewMessage(chatID, text)
	m.ParseMode = "HTML"
	_, _ = b.api.Send(m)
}

func (b *Bot) sendPlain(chatID int64, text string) {
	m := tgbotapi.NewMessage(chatID, text)
	_, _ = b.api.Send(m)
}
