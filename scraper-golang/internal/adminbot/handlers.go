package adminbot

import (
	"fmt"
	"html"
	"io"
	"net/http"
	"strconv"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/mailer"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

const emailsPerPage = 15

func (b *Bot) answerCallback(cbID, text string) {
	_, err := b.api.Request(tgbotapi.NewCallback(cbID, text))
	if err != nil {
		prettylog.Warnf("ответ на колбэк (answerCallbackQuery) · %v", err)
	} else if strings.TrimSpace(text) != "" {
		prettylog.Adminf("· всплывашка · %s", previewRunes(text, 72))
	}
}

func (b *Bot) handleUpdate(u tgbotapi.Update) {
	switch {
	case u.CallbackQuery != nil:
		prettylog.Adminf("апдейт · inline-кнопка · id=%s", u.CallbackQuery.ID)
		b.onCallback(u.CallbackQuery)
	case u.Message != nil:
		m := u.Message
		kind := "текст"
		if m.Document != nil {
			kind = fmt.Sprintf("документ %q", m.Document.FileName)
		} else if len(m.Photo) > 0 {
			kind = "фото"
		}
		prettylog.Adminf("апдейт · сообщение · %s · chat=%d", kind, m.Chat.ID)
		b.onMessage(m)
	default:
		prettylog.Admin("апдейт · без callback/message", "")
	}
}

func (b *Bot) onCallback(cb *tgbotapi.CallbackQuery) {
	data := cb.Data
	var chID, fromUID int64
	if cb.Message != nil {
		chID = cb.Message.Chat.ID
	}
	if cb.From != nil {
		fromUID = cb.From.ID
	}

	if cb.Message == nil || !b.isAdminChat(cb.Message.Chat.ID) {
		prettylog.Adminf("кнопка игнор (не админ-чат) · chat=%d user=%d · data=%q", chID, fromUID, data)
		b.answerCallback(cb.ID, "")
		return
	}

	chatID := cb.Message.Chat.ID
	msgID := cb.Message.MessageID
	prettylog.Adminf("кнопка · chat=%d msg=%d user=%d · data=%q", chatID, msgID, fromUID, data)

	switch {
	case data == "admin_main":
		prettylog.Admin("экран · главная панель", "")
		b.clearDialog()
		b.editHTML(chatID, msgID, mainPanelHTML(), kbMain())
	case data == "admin_pending":
		prettylog.Admin("экран · ожидают подтверждения", "")
		b.clearDialog()
		b.showPending(chatID, msgID)
	case strings.HasPrefix(data, "approve_"):
		b.clearDialog()
		uid, _ := strconv.ParseInt(strings.TrimPrefix(data, "approve_"), 10, 64)
		_ = listingsdb.AuthorizeUser(b.db, uid)
		prettylog.OKf("воркер %d одобрен", uid)
		b.notifyWorkerApproved(uid)
		b.showPending(chatID, msgID)
		b.answerCallback(cb.ID, "✅ Воркер одобрен")
		return
	case strings.HasPrefix(data, "reject_"):
		b.clearDialog()
		uid, _ := strconv.ParseInt(strings.TrimPrefix(data, "reject_"), 10, 64)
		_ = listingsdb.BlockUser(b.db, uid)
		prettylog.Warnf("заявка отклонена (блок) · user_id=%d", uid)
		b.showPending(chatID, msgID)
		b.answerCallback(cb.ID, "❌ Отклонён")
		return
	case data == "admin_workers":
		prettylog.Admin("экран · воркеры", "")
		b.clearDialog()
		b.showWorkers(chatID, msgID)
	case strings.HasPrefix(data, "block_"):
		uid, _ := strconv.ParseInt(strings.TrimPrefix(data, "block_"), 10, 64)
		_ = listingsdb.BlockUser(b.db, uid)
		prettylog.Warnf("воркер заблокирован · user_id=%d", uid)
		b.showWorkers(chatID, msgID)
		b.answerCallback(cb.ID, "🚫 Заблокирован")
		return
	case strings.HasPrefix(data, "delete_"):
		uid, _ := strconv.ParseInt(strings.TrimPrefix(data, "delete_"), 10, 64)
		ok, _ := listingsdb.DeleteUser(b.db, uid)
		if ok {
			prettylog.OKf("воркер удалён из БД · user_id=%d", uid)
		} else {
			prettylog.Warnf("удаление воркера · user_id=%d не найден", uid)
		}
		b.showWorkers(chatID, msgID)
		if ok {
			b.answerCallback(cb.ID, "🗑 Удалён")
		} else {
			b.answerCallback(cb.ID, "Не найден")
		}
		return
	case data == "admin_blocked":
		prettylog.Admin("экран · заблокированные", "")
		b.clearDialog()
		b.showBlocked(chatID, msgID)
	case strings.HasPrefix(data, "unblock_"):
		uid, _ := strconv.ParseInt(strings.TrimPrefix(data, "unblock_"), 10, 64)
		_ = listingsdb.UnblockUser(b.db, uid)
		prettylog.OKf("снята блокировка · user_id=%d", uid)
		b.showBlocked(chatID, msgID)
		b.answerCallback(cb.ID, "🔓 Разблокирован")
		return
	case data == "admin_emails":
		prettylog.Admin("экран · меню почт", "")
		b.clearDialog()
		b.showEmailsMenu(chatID, msgID)
	case data == "emails_add":
		prettylog.Admin("диалог · ввод почт построчно", "")
		b.dlgMu.Lock()
		b.dlgStep = "emails"
		b.dlgMu.Unlock()
		b.editHTML(chatID, msgID, emailsAddHTML(), tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "admin_emails")),
		))
	case data == "emails_upload":
		prettylog.Admin("экран · загрузка CSV", "")
		b.clearDialog()
		b.editHTML(chatID, msgID, emailsUploadHTML(), tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ К меню почт", "admin_emails")),
		))
	case strings.HasPrefix(data, "emails_list_"):
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "emails_list_"))
		prettylog.Adminf("экран · список почт · страница %d", page)
		b.showEmailsList(chatID, msgID, page)
	case strings.HasPrefix(data, "emails_unblock_"):
		rest := strings.TrimPrefix(data, "emails_unblock_")
		page, email := parsePageAndSafeEmail(rest)
		em := decodeEmailFromCallback(email)
		_, _ = listingsdb.UnblockEmail(b.db, b.ownerID, em)
		prettylog.OKf("почта разблокирована · %s · стр.%d", em, page)
		b.showEmailsList(chatID, msgID, page)
		b.answerCallback(cb.ID, "↩️ Разблокировано")
		return
	case strings.HasPrefix(data, "emails_del_"):
		rest := strings.TrimPrefix(data, "emails_del_")
		page, emailSafe := parsePageAndSafeEmail(rest)
		em := decodeEmailFromCallback(emailSafe)
		_, _ = listingsdb.DeleteEmail(b.db, b.ownerID, em)
		n, _ := listingsdb.EmailsTotalCount(b.db, b.ownerID)
		if page > 0 && n <= page*emailsPerPage {
			page = max(0, page-1)
		}
		prettylog.Warnf("почта удалена из базы · %s · стр.%d", em, page)
		b.showEmailsList(chatID, msgID, page)
		b.answerCallback(cb.ID, "🗑 Удалено")
		return
	case data == "emails_test":
		email, pass, ok := listingsdb.RandomActiveEmail(b.db, b.ownerID)
		if !ok {
			prettylog.Admin("тест почты · нет активных записей", "")
			b.answerCallback(cb.ID, "Нет доступных почт")
			return
		}
		prettylog.Adminf("тест почты SMTP · с %s", email)
		b.answerCallback(cb.ID, "Отправляю…")
		if mailer.SendTestEmail(b.db, email, pass, b.ownerID, "") {
			prettylog.OK("тест почты · отправлено", email)
			b.sendHTML(chatID, fmt.Sprintf("✅ Тест OK\n\nС <code>%s</code> на %s", html.EscapeString(email), html.EscapeString(mailer.TestRecipient())))
		} else {
			prettylog.Warn("тест почты · ошибка отправки", email)
			b.sendHTML(chatID, fmt.Sprintf("❌ Ошибка с <code>%s</code>", html.EscapeString(email)))
		}
		return
	case data == "emails_test_all":
		n, _ := listingsdb.EmailsTotalCount(b.db, b.ownerID)
		if n == 0 {
			prettylog.Admin("тест всех почт · база пуста", "")
			b.answerCallback(cb.ID, "Нет почт")
			return
		}
		prettylog.Adminf("тест всех почт · записей %d", n)
		b.answerCallback(cb.ID, "Тестирую…")
		okN, failN, failed := mailer.TestAllEmailsForAdmin(b.db, b.ownerID)
		prettylog.Adminf("тест всех почт · готово · OK=%d fail=%d", okN, failN)
		if len(failed) > 0 {
			prettylog.Warnf("заблокированы после теста · %v", previewRunes(strings.Join(failed, ", "), 120))
		}
		var sb strings.Builder
		sb.WriteString("🔄 <b>Тест всех почт</b> (на ")
		sb.WriteString(html.EscapeString(mailer.TestRecipient()))
		sb.WriteString(")\n\n✅ Работают: ")
		sb.WriteString(strconv.Itoa(okN))
		sb.WriteString("\n❌ Не работают: ")
		sb.WriteString(strconv.Itoa(failN))
		if len(failed) > 0 {
			sb.WriteString("\n\nЗаблокированы:\n")
			for i, e := range failed {
				if i >= 10 {
					sb.WriteString(fmt.Sprintf("\n… и ещё %d", len(failed)-10))
					break
				}
				sb.WriteString("• <code>")
				sb.WriteString(html.EscapeString(e))
				sb.WriteString("</code>\n")
			}
		}
		b.sendHTML(chatID, sb.String())
		return
	case data == "emails_export":
		rows, err := listingsdb.ListEmails(b.db, b.ownerID, 10000, 0)
		if err != nil || len(rows) == 0 {
			prettylog.Admin("экспорт CSV · нет данных", "")
			b.answerCallback(cb.ID, "Нет почт")
			return
		}
		var sb strings.Builder
		sb.WriteString("email,password\n")
		for _, r := range rows {
			sb.WriteString(r.Email)
			sb.WriteByte(',')
			sb.WriteString(r.Password)
			sb.WriteByte('\n')
		}
		prettylog.Adminf("экспорт CSV · строк %d → sendDocument", len(rows))
		doc := tgbotapi.NewDocument(chatID, tgbotapi.FileBytes{Name: "emails_export.csv", Bytes: []byte(sb.String())})
		doc.Caption = fmt.Sprintf("📥 Экспорт: %d почт", len(rows))
		if _, err := b.api.Send(doc); err != nil {
			prettylog.Warnf("sendDocument (экспорт) · %v", err)
		} else {
			prettylog.OK("файл экспорта отправлен в чат", fmt.Sprintf("%d почт", len(rows)))
		}
		b.answerCallback(cb.ID, "📥 Файл отправлен")
		return
	case data == "admin_templates":
		prettylog.Admin("экран · шаблоны писем", "")
		b.clearDialog()
		b.showTemplates(chatID, msgID)
	case data == "tpl_add":
		prettylog.Admin("диалог · новый шаблон (шаг название)", "")
		b.dlgMu.Lock()
		b.dlgStep = "tpl_name"
		b.dlgEditID = 0
		b.dlgTplName = ""
		b.dlgTplSubject = ""
		b.dlgMu.Unlock()
		b.editHTML(chatID, msgID, tplAddStep1HTML(), tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("❌ Отмена", "admin_templates")),
		))
	case strings.HasPrefix(data, "tpl_edit_"):
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "tpl_edit_"), 10, 64)
		name, _, _, ok := listingsdb.GetEmailTemplate(b.db, b.ownerID, tid)
		if !ok {
			prettylog.Warnf("меню правки шаблона · id=%d не найден", tid)
			b.answerCallback(cb.ID, "Не найден")
			return
		}
		prettylog.Adminf("шаблон · меню правки id=%d name=%q", tid, name)
		b.editHTML(chatID, msgID, fmt.Sprintf("✏️ <b>%s</b>\n\nЧто изменить?", html.EscapeString(name)),
			tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("Тема", fmt.Sprintf("tpl_edsubj_%d", tid)),
					tgbotapi.NewInlineKeyboardButtonData("Текст", fmt.Sprintf("tpl_edbody_%d", tid)),
				),
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ К шаблонам", "admin_templates")),
			))
		b.answerCallback(cb.ID, "")
		return
	case strings.HasPrefix(data, "tpl_edsubj_"):
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "tpl_edsubj_"), 10, 64)
		name, subj, _, ok := listingsdb.GetEmailTemplate(b.db, b.ownerID, tid)
		if !ok {
			b.answerCallback(cb.ID, "Не найден")
			return
		}
		b.dlgMu.Lock()
		b.dlgStep = "tpl_editsubj"
		b.dlgEditID = tid
		b.dlgTplName = name
		b.dlgMu.Unlock()
		b.editHTML(chatID, msgID,
			fmt.Sprintf("📝 <b>Тема письма</b> — «%s»\n\nОтправьте новую тему (переменные <code>{title}</code> и др.). Пусто или <code>-</code> — в письме только название товара.", html.EscapeString(name)),
			tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Отмена", "admin_templates")),
			))
		b.sendHTML(chatID, "<pre>"+html.EscapeString(subj)+"</pre>")
		b.answerCallback(cb.ID, "")
		return
	case strings.HasPrefix(data, "tpl_edbody_"):
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "tpl_edbody_"), 10, 64)
		name, subj, body, ok := listingsdb.GetEmailTemplate(b.db, b.ownerID, tid)
		if !ok {
			b.answerCallback(cb.ID, "Не найден")
			return
		}
		b.dlgMu.Lock()
		b.dlgStep = "tpl_body"
		b.dlgEditID = tid
		b.dlgTplName = name
		b.dlgTplSubject = subj
		b.dlgMu.Unlock()
		b.editHTML(chatID, msgID, fmt.Sprintf("✏️ <b>Текст письма</b> — «%s»\n\nОтправьте новый текст:", html.EscapeString(name)),
			tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("Отмена", "admin_templates")),
			))
		b.sendHTML(chatID, "<pre>"+html.EscapeString(body)+"</pre>")
		b.answerCallback(cb.ID, "")
		return
	case strings.HasPrefix(data, "tpl_del_"):
		tid, _ := strconv.ParseInt(strings.TrimPrefix(data, "tpl_del_"), 10, 64)
		_ = listingsdb.ClearActiveTemplateIf(b.db, b.ownerID, tid)
		ok, _ := listingsdb.DeleteEmailTemplate(b.db, b.ownerID, tid)
		if ok {
			prettylog.Warnf("шаблон удалён · id=%d", tid)
		} else {
			prettylog.Warnf("удаление шаблона · id=%d не найден", tid)
		}
		b.showTemplates(chatID, msgID)
		if ok {
			b.answerCallback(cb.ID, "🗑 Удалён")
		} else {
			b.answerCallback(cb.ID, "Не найден")
		}
		return
	default:
		prettylog.Adminf("кнопка без обработчика · %q", data)
		b.answerCallback(cb.ID, "")
		return
	}
	b.answerCallback(cb.ID, "")
}

func parsePageAndSafeEmail(rest string) (page int, safe string) {
	i := strings.IndexByte(rest, '_')
	if i < 0 {
		return 0, rest
	}
	page, _ = strconv.Atoi(rest[:i])
	return page, rest[i+1:]
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (b *Bot) onMessage(msg *tgbotapi.Message) {
	if !b.isAdminChat(msg.Chat.ID) {
		prettylog.Adminf("сообщение игнор · chat=%d (не админ)", msg.Chat.ID)
		return
	}

	b.dlgMu.Lock()
	step := b.dlgStep
	b.dlgMu.Unlock()

	if msg.Document != nil && msg.Document.FileName != "" && strings.HasSuffix(strings.ToLower(msg.Document.FileName), ".csv") {
		prettylog.Adminf("загрузка CSV · файл=%q · шаг диалога=%q", msg.Document.FileName, step)
		b.handleCSVUpload(msg)
		return
	}
	if msg.Text == "" {
		prettylog.Adminf("сообщение без текста · chat=%d · шаг=%q", msg.Chat.ID, step)
		return
	}

	prettylog.Adminf("текст от админа · chat=%d · шаг=%q · %s", msg.Chat.ID, step, previewOneLine(msg.Text, 160))

	if msg.IsCommand() && msg.Command() == "start" {
		prettylog.OK("команда /start · главная панель", "")
		b.clearDialog()
		m := tgbotapi.NewMessage(msg.Chat.ID, mainPanelHTML())
		m.ParseMode = "HTML"
		m.ReplyMarkup = kbMain()
		if _, err := b.api.Send(m); err != nil {
			prettylog.Warnf("sendMessage (/start) · %v", err)
		} else {
			prettylog.Admin("→ отправлена главная панель (reply keyboard)", "")
		}
		return
	}

	switch step {
	case "emails":
		pairs := ParseEmailsText(msg.Text)
		if len(pairs) == 0 {
			prettylog.Warn("ввод почт · нет валидных строк", previewOneLine(msg.Text, 80))
			b.sendHTML(msg.Chat.ID, "❌ Не найдено валидных строк. Формат: mail@gmail.com:apppassword")
			return
		}
		added, skipped := listingsdb.AddEmailsBatch(b.db, b.ownerID, pairs)
		prettylog.OKf("почты добавлены · +%d дублей пропущено %d", added, skipped)
		b.clearDialog()
		b.sendHTML(msg.Chat.ID, fmt.Sprintf("✅ Добавлено: %d, пропущено (дубли): %d", added, skipped))
		n, _ := listingsdb.EmailsTotalCount(b.db, b.ownerID)
		m := tgbotapi.NewMessage(msg.Chat.ID, fmt.Sprintf("📧 <b>База почт</b>\n\nВсего: %d", n))
		m.ParseMode = "HTML"
		m.ReplyMarkup = emailsMenuKB(b.db, b.ownerID)
		if _, err := b.api.Send(m); err != nil {
			prettylog.Warnf("sendMessage (меню почт) · %v", err)
		} else {
			prettylog.Adminf("→ меню почт · всего %d", n)
		}
	case "tpl_name":
		name := strings.TrimSpace(msg.Text)
		if name == "" {
			prettylog.Admin("шаблон · пустое имя — просим повтор", "")
			b.sendHTML(msg.Chat.ID, "Введите название")
			return
		}
		b.dlgMu.Lock()
		b.dlgTplName = name
		b.dlgStep = "tpl_subject"
		b.dlgMu.Unlock()
		prettylog.Adminf("шаблон · название принято · %q → шаг тема", name)
		vars := strings.Builder{}
		for k := range TemplateVarDescriptions {
			if vars.Len() > 0 {
				vars.WriteString(", ")
			}
			vars.WriteString("<code>{")
			vars.WriteString(k)
			vars.WriteString("}</code>")
		}
		b.sendHTML(msg.Chat.ID, "Шаг 2/3: введите <b>тему письма</b>.\n\nПусто или <code>-</code> — только название товара.\n\nПеременные: "+vars.String())
	case "tpl_subject":
		b.dlgMu.Lock()
		b.dlgTplSubject = strings.TrimSpace(msg.Text)
		b.dlgStep = "tpl_body"
		name := b.dlgTplName
		b.dlgMu.Unlock()
		prettylog.Adminf("шаблон · тема принята · %q → шаг тело", name)
		vars := strings.Builder{}
		for k := range TemplateVarDescriptions {
			if vars.Len() > 0 {
				vars.WriteString(", ")
			}
			vars.WriteString("<code>{")
			vars.WriteString(k)
			vars.WriteString("}</code>")
		}
		b.sendHTML(msg.Chat.ID, "Шаг 3/3: введите <b>текст шаблона</b>.\n\nПеременные: "+vars.String())
	case "tpl_editsubj":
		newSubj := strings.TrimSpace(msg.Text)
		b.dlgMu.Lock()
		name := b.dlgTplName
		eid := b.dlgEditID
		b.dlgMu.Unlock()
		if eid <= 0 {
			b.clearDialog()
			return
		}
		_, _, body, ok := listingsdb.GetEmailTemplate(b.db, b.ownerID, eid)
		if !ok {
			prettylog.Warnf("шаблон · тема id=%d — запись пропала", eid)
			b.clearDialog()
			return
		}
		_, _ = listingsdb.UpdateEmailTemplate(b.db, b.ownerID, eid, name, newSubj, body)
		prettylog.OKf("тема шаблона обновлена · id=%d · %q", eid, name)
		b.sendHTML(msg.Chat.ID, fmt.Sprintf("Тема «%s» обновлена", html.EscapeString(name)))
		b.clearDialog()
		b.sendHTMLWithKB(msg.Chat.ID, templatesListHTML(b.db, b.ownerID), templatesListKB(b.db, b.ownerID))
	case "tpl_body":
		body := msg.Text
		b.dlgMu.Lock()
		name := b.dlgTplName
		eid := b.dlgEditID
		subj := b.dlgTplSubject
		b.dlgMu.Unlock()
		if eid > 0 {
			_, _, _, ok := listingsdb.GetEmailTemplate(b.db, b.ownerID, eid)
			if !ok {
				prettylog.Warnf("шаблон · обновление id=%d — запись пропала", eid)
				b.clearDialog()
				return
			}
			_, _ = listingsdb.UpdateEmailTemplate(b.db, b.ownerID, eid, name, subj, body)
			prettylog.OKf("шаблон обновлён · id=%d · %q", eid, name)
			b.sendHTML(msg.Chat.ID, fmt.Sprintf("✅ Шаблон «%s» обновлён", html.EscapeString(name)))
		} else {
			_, _ = listingsdb.AddEmailTemplate(b.db, b.ownerID, name, subj, body)
			prettylog.OKf("шаблон создан · %q · тело %d симв.", name, len([]rune(body)))
			b.sendHTML(msg.Chat.ID, fmt.Sprintf("✅ Шаблон «%s» добавлен", html.EscapeString(name)))
		}
		b.clearDialog()
		b.sendHTMLWithKB(msg.Chat.ID, templatesListHTML(b.db, b.ownerID), templatesListKB(b.db, b.ownerID))
	default:
		prettylog.Adminf("текст вне диалога (шаг пустой) · %s", previewOneLine(msg.Text, 120))
	}
}

func (b *Bot) handleCSVUpload(msg *tgbotapi.Message) {
	fn := msg.Document.FileName
	prettylog.Adminf("CSV · getFile · %q", fn)
	fileCfg := tgbotapi.FileConfig{FileID: msg.Document.FileID}
	file, err := b.api.GetFile(fileCfg)
	if err != nil {
		prettylog.Warnf("CSV · getFile · %v", err)
		b.sendHTML(msg.Chat.ID, "❌ Файл: "+err.Error())
		return
	}
	url := file.Link(b.api.Token)
	prettylog.Adminf("CSV · скачивание · %s", url)
	resp, err := http.Get(url)
	if err != nil {
		prettylog.Warnf("CSV · HTTP · %v", err)
		b.sendHTML(msg.Chat.ID, "❌ Скачивание: "+err.Error())
		return
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		prettylog.Warnf("CSV · чтение тела · %v", err)
		b.sendHTML(msg.Chat.ID, "❌ Чтение: "+err.Error())
		return
	}
	pairs := ParseEmailsCSV(string(data))
	if len(pairs) == 0 {
		prettylog.Warn("CSV · не распознаны строки email", fmt.Sprintf("%d байт", len(data)))
		b.sendHTML(msg.Chat.ID, "❌ В CSV не найдено email (колонки email, apppassword)")
		return
	}
	added, skipped := listingsdb.AddEmailsBatch(b.db, b.ownerID, pairs)
	prettylog.OKf("CSV импорт · +%d пропуск %d · файл %q", added, skipped, fn)
	b.sendHTML(msg.Chat.ID, fmt.Sprintf("✅ Из CSV: добавлено %d, пропущено %d", added, skipped))
}

func (b *Bot) notifyWorkerApproved(userID int64) {
	if b.client == nil {
		prettylog.Adminf("одобрение · push воркеру %d пропущен (нет CLIENT_BOT_TOKEN)", userID)
		return
	}
	text := "✅ <b>Ваша заявка одобрена!</b>\n\nТеперь вы можете пользоваться ботом. Нажмите /start для начала."
	m := tgbotapi.NewMessage(userID, text)
	m.ParseMode = "HTML"
	prettylog.Adminf("→ клиентский бот · уведомление user_id=%d", userID)
	if _, err := b.client.Send(m); err != nil {
		prettylog.Warnf("клиентский бот · user_id=%d · %v", userID, err)
	} else {
		prettylog.OK("воркер уведомлён (клиентский бот)", fmt.Sprintf("user_id=%d", userID))
	}
}

func (b *Bot) editHTML(chatID int64, msgID int, text string, kb tgbotapi.InlineKeyboardMarkup) {
	prettylog.Adminf("→ editMessageText · chat=%d msg=%d · %s", chatID, msgID, previewOneLine(text, 100))
	edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, msgID, text, kb)
	edit.ParseMode = "HTML"
	if _, err := b.api.Send(edit); err != nil {
		prettylog.Warnf("editMessageText · %v", err)
	}
}

func (b *Bot) sendHTML(chatID int64, text string) {
	prettylog.Adminf("→ sendMessage HTML · chat=%d · %s", chatID, previewOneLine(text, 120))
	m := tgbotapi.NewMessage(chatID, text)
	m.ParseMode = "HTML"
	if _, err := b.api.Send(m); err != nil {
		prettylog.Warnf("sendMessage · %v", err)
	}
}

func (b *Bot) sendHTMLWithKB(chatID int64, text string, kb tgbotapi.InlineKeyboardMarkup) {
	prettylog.Adminf("→ sendMessage HTML+клавиатура · chat=%d · %s", chatID, previewOneLine(text, 100))
	m := tgbotapi.NewMessage(chatID, text)
	m.ParseMode = "HTML"
	m.ReplyMarkup = kb
	if _, err := b.api.Send(m); err != nil {
		prettylog.Warnf("sendMessage+KB · %v", err)
	}
}
