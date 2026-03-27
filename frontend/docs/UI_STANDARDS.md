# UI Standards (Locked Baseline)

Этот документ фиксирует базовую логику верстки для всех новых страниц.

## 1) Контентная ширина

- Базовая ширина: `--content-max-regular: 1280px`
- Широкая ширина: `--content-max-wide: calc(--content-max-regular * 1.2)`
- Это и есть правило `+20%` для широкого контента.
- Все основные блоки должны рендериться в пределах `var(--content-max-wide)` и центрироваться.

## 2) Структура страницы

Использовать `std-*` классы из `src/styles/globals.css`:

- `std-page`
- `std-page-head`, `std-page-head-inner`, `std-page-title`, `std-page-subtitle`
- `std-section`, `std-section-head`, `std-section-body`
- `std-panel`, `std-panel-head`, `std-panel-title`, `std-panel-subtitle`

## 3) Табличный стандарт

Для всех операционных таблиц:

- `std-table-wrap` + `std-table`
- Все заголовки и значения по умолчанию центрируются
- Межколоночный разделитель: тонкая вертикальная линия
- Колонки служебных зон:
  - `col-toggle` ≈ 11%
  - `col-status` ≈ 24%
  - `col-actions` ≈ 8%
- Для специальных выравниваний:
  - `is-left`, `is-right`
  - `std-table-cell-status`
  - `std-table-cell-actions`

## 4) Кнопки действий

- Кнопки под таблицей: `std-panel-actions`
- Кнопки удаления с текстом, без иконки-крестика в таблицах, если это primary destructive action.

## 5) Принцип применения

- Новые страницы: сразу строить на `std-*` без локального «изобретения» сетки.
- Локальный CSS страницы только для доменных нюансов, а не для повторного описания базовой сетки.
