package adminbot

import "github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"

// TemplateVarDescriptions подписи переменных (как TEMPLATE_VARS в Python).
var TemplateVarDescriptions = map[string]string{
	"url":           "Ссылка на объявление",
	"title":         "Название товара",
	"price":         "Цена (€X.XX)",
	"price_cents":   "Цена в центах",
	"seller_name":   "Имя продавца",
	"city":          "Город",
	"category":      "Категория",
	"description":   "Описание (первые 500 символов)",
	"user_name":     "Имя отправителя (из почты)",
	"item_id":       "ID объявления",
}

func exampleTemplateVars() map[string]string {
	return map[string]string{
		"url":           "https://marktplaats.nl/v/example/m1234567890",
		"title":         "iPhone 14 Pro",
		"price":         "€899.00",
		"price_cents":   "89900",
		"seller_name":   "Jan",
		"city":          "Amsterdam",
		"category":      "Телекоммуникация",
		"description":   "Отличное состояние, мало использовался...",
		"user_name":     "Мария",
		"item_id":       "m1234567890",
	}
}

// FormatTemplateExampleBody пример с подставленными значениями.
func FormatTemplateExampleBody(body string) string {
	return listingsdb.FormatTemplate(body, exampleTemplateVars())
}

func templateExampleBody() string {
	return "Привет! Меня зовут {user_name}.\n" +
		"Хотела бы купить ваш товар «{title}» ({price}).\n" +
		"Ссылка: {url}\n\nС уважением."
}

func templateExampleSubject() string {
	return "Вопрос по «{title}» — {price}"
}

// TemplateExampleBody публичный текст примера (для clientbot / документации).
func TemplateExampleBody() string { return templateExampleBody() }

// TemplateExampleSubject пример темы письма с плейсхолдерами.
func TemplateExampleSubject() string { return templateExampleSubject() }

// TemplateExampleSubjectFilled тема с подстановкой (для подсказок в боте).
func TemplateExampleSubjectFilled() string {
	return listingsdb.FormatTemplate(templateExampleSubject(), exampleTemplateVars())
}
