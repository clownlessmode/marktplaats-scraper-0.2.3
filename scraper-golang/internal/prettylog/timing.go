package prettylog

import (
	"fmt"
	"time"
)

// FormatDuration кратко для логов (мс, с, мин).
func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%d µs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.0f мс", float64(d.Microseconds())/1000)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.2f с", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%d мин %d с", m, s)
}

// Timer печатает строку ТАЙМЕР с длительностью при вызове возвращённой функции.
// Использование: defer prettylog.Timer("загрузка категорий")()
func Timer(label string) (done func()) {
	t0 := time.Now()
	return func() {
		line(cyan(padTag("ТАЙМЕР")), label, FormatDuration(time.Since(t0)))
	}
}

// TotalElapsed — итоговое время программы (в конце main).
func TotalElapsed(start time.Time) {
	line(cyan(padTag("ИТОГ")), "время работы программы", FormatDuration(time.Since(start)))
}

// TimingNote одна строка без defer (уже известная длительность).
func TimingNote(label string, d time.Duration) {
	line(cyan(padTag("ТАЙМЕР")), label, FormatDuration(d))
}
