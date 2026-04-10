#!/usr/bin/env bash
# Сборка всех бинарников под Linux (статическая линковка, без CGO).
# Использование:
#   ./scripts/build-linux.sh           # amd64 → dist/linux-amd64/
#   ./scripts/build-linux.sh arm64     # arm64 → dist/linux-arm64/
#   ./scripts/build-linux.sh all       # оба

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export CGO_ENABLED=0
LDFLAGS='-s -w'

build_one() {
  local goarch=$1
  local outdir="$ROOT/dist/linux-${goarch}"
  mkdir -p "$outdir"
  echo "==> GOOS=linux GOARCH=${goarch} → ${outdir}/"
  # marktplaats-playwright = скрапер Marktplaats (Playwright); отдельного «scraper» нет
  for cmd in adminbot clientbot marktplaats-playwright; do
    echo "    $cmd"
    GOOS=linux GOARCH="${goarch}" go build -trimpath -ldflags="${LDFLAGS}" -o "${outdir}/${cmd}" "./cmd/${cmd}"
  done
  echo "Готово: $(ls -1 "$outdir" | tr '\n' ' ')"
}

case "${1:-amd64}" in
  amd64) build_one amd64 ;;
  arm64) build_one arm64 ;;
  all)
    build_one amd64
    build_one arm64
    ;;
  *)
    echo "Неизвестная архитектура: $1 (ожидается amd64, arm64 или all)" >&2
    exit 1
    ;;
esac

echo
echo "На сервере: положите рядом .env, proxies.txt (если нужны), bot.db; для marktplaats-playwright установите Chromium:"
echo "  go run github.com/playwright-community/playwright-go/cmd/playwright@latest install chromium"
echo "(или скачайте браузер тем же способом на Linux-хосте.)"
