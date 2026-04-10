package marktplaats

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/playwright-community/playwright-go"
	"github.com/tidwall/gjson"
)

// ParentCategoriesFromHTML parses li.CategoriesBlock-listItem (Python get_parent_categories).
func ParentCategoriesFromHTML(html string, baseURL string) ([]Category, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}
	var out []Category
	doc.Find("li.CategoriesBlock-listItem").Each(func(i int, s *goquery.Selection) {
		a := s.Find("a.hz-Link--navigation").First()
		if a.Length() == 0 {
			return
		}
		href, ok := a.Attr("href")
		if !ok || href == "" {
			return
		}
		// Python: href.split("/") -> ['', 'l', categoryId, slug]
		parts := strings.Split(href, "/")
		if len(parts) < 4 || parts[1] != "l" {
			return
		}
		catID, err := parseInt(parts[2])
		if err != nil {
			return
		}
		slug := parts[3]
		out = append(out, Category{
			ID:  catID,
			URL: strings.TrimRight(baseURL, "/") + "/l/" + slug,
		})
	})
	return out, nil
}

// ParentCategoriesFromConfigJSON fills categories from window.__CONFIG__.categoryLinks (Python fallback).
func ParentCategoriesFromConfigJSON(configJSON string, baseURL string) ([]Category, error) {
	if configJSON == "" {
		return nil, nil
	}
	links := gjson.Get(configJSON, "categoryLinks").Array()
	base := strings.TrimRight(baseURL, "/")
	var out []Category
	for _, link := range links {
		catID := int(link.Get("id").Int())
		urlPath := link.Get("url").String()
		if !strings.HasPrefix(urlPath, "/cp/") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(urlPath, "/"), "/")
		if len(parts) < 3 {
			continue
		}
		slug := parts[len(parts)-1]
		if slug == "" {
			continue
		}
		out = append(out, Category{
			ID:  catID,
			URL: base + "/l/" + slug + "/",
		})
	}
	return out, nil
}

// EvalWindowConfig returns JSON string of window.__CONFIG__ or empty.
func EvalWindowConfig(page playwright.Page) (string, error) {
	v, err := page.Evaluate(`() => {
		const c = window.__CONFIG__;
		return c ? JSON.stringify(c) : "";
	}`)
	if err != nil {
		return "", err
	}
	s, _ := v.(string)
	return s, nil
}

// SubcategoriesFromHTML parses select#categoryId options + a.category-name (Python __get_subcategories).
func SubcategoriesFromHTML(html string, parent Category) ([]Category, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}
	sel := doc.Find("select#" + SelectCategoryID).First()
	if sel.Length() == 0 {
		return nil, nil
	}
	if doc.Find("div#"+itoa(parent.ID)).First().Length() == 0 {
		return nil, nil
	}

	hrefs := map[string]string{}
	doc.Find("a.category-name").Each(func(i int, a *goquery.Selection) {
		href, ok := a.Attr("href")
		if !ok {
			return
		}
		name := strings.TrimSpace(a.Text())
		if name != "" {
			hrefs[name] = href
		}
	})

	var out []Category
	sel.Find("option").Each(func(i int, opt *goquery.Selection) {
		val, ok := opt.Attr("value")
		if !ok || val == "" {
			return
		}
		subID, err := parseInt(val)
		if err != nil {
			return
		}
		if subID == parent.ID || subID == AllCategoriesID {
			return
		}
		name := strings.TrimSpace(opt.Text())
		subHref, ok := hrefs[name]
		if !ok {
			return
		}
		u, err := url.Parse(subHref)
		if err != nil {
			return
		}
		if !u.IsAbs() {
			u, _ = url.Parse(BaseURL + subHref)
		}
		out = append(out, Category{ID: subID, URL: u.String()})
	})
	return out, nil
}

func stripCounterDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '.' || r == ',' || r == '(' || r == ')' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

// ListingsCountFromHTML parses counter next to offeredSince-Altijd (Python listings_count).
func ListingsCountFromHTML(html string) (int, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return 0, err
	}
	label := doc.Find(`label[for="offeredSince-Altijd"]`).First()
	if label.Length() == 0 {
		return 0, fmt.Errorf("метка счётчика не найдена")
	}
	counter := label.Find("span.hz-SelectionInput-Counter span.hz-Text").First()
	if counter.Length() == 0 {
		return 0, fmt.Errorf("элемент счётчика не найден")
	}
	text := stripCounterDigits(strings.TrimSpace(counter.Text()))
	text = strings.ReplaceAll(text, " ", "")
	var n int
	_, err = fmt.Sscanf(text, "%d", &n)
	return n, err
}
