# Yandex MCP Server

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MCP](https://img.shields.io/badge/MCP-compatible-green.svg)](https://modelcontextprotocol.io/)

MCP (Model Context Protocol) сервер для управления рекламными кампаниями в **Yandex Direct** и аналитикой в **Yandex Metrika** через Claude, Cursor и другие MCP-совместимые клиенты.

> Управляй рекламой и аналитикой Яндекса голосом через AI

## Возможности

### Yandex Direct API v5
- **Кампании**: получение, создание, обновление, приостановка, возобновление, архивирование
- **Группы объявлений**: получение, создание, обновление
- **Объявления**: получение, создание, модерация, управление состоянием
- **Ключевые слова**: получение, добавление, удаление, управление ставками
- **Статистика**: детальные отчёты по кампаниям и группам

### Yandex Metrika API
- **Счётчики**: получение списка, создание, удаление
- **Цели**: получение, создание
- **Отчёты**: статистика по метрикам с группировкой по времени

## Быстрый старт

### 1. Установка

```bash
# С помощью pip
pip install yandex-mcp

# Или с помощью uv (рекомендуется)
uv pip install yandex-mcp

# Или из исходников
git clone https://github.com/SvechaPVL/yandex-mcp.git
cd yandex-mcp
pip install -e .
```

### 2. Получение токена

1. Зарегистрируйте приложение на [OAuth Yandex](https://oauth.yandex.ru/)
2. Добавьте права: `direct:api`, `metrika:read`, `metrika:write`
3. Получите OAuth токен

### 3. Настройка Claude Code

Добавьте в `~/.claude.json` (или через `claude mcp add`):

```json
{
  "mcpServers": {
    "yandex": {
      "command": "yandex-mcp",
      "env": {
        "YANDEX_TOKEN": "your_oauth_token"
      }
    }
  }
}
```

### 4. Готово!

```
> Покажи все мои кампании в Директе
> Приостанови кампанию 12345
> Какая статистика по сайту за неделю?
```

## Переменные окружения

```bash
# Единый токен (рекомендуется)
YANDEX_TOKEN=your_oauth_token

# Или раздельные токены
YANDEX_DIRECT_TOKEN=your_direct_token
YANDEX_METRIKA_TOKEN=your_metrika_token

# Для агентских аккаунтов
YANDEX_CLIENT_LOGIN=client_login

# Песочница для тестов
YANDEX_USE_SANDBOX=true
```

## Доступные инструменты (33 шт)

### Yandex Direct

| Инструмент | Описание |
|------------|----------|
| `direct_get_campaigns` | Получить список кампаний |
| `direct_update_campaign` | Обновить настройки кампании |
| `direct_suspend_campaigns` | Приостановить кампании |
| `direct_resume_campaigns` | Возобновить кампании |
| `direct_archive_campaigns` | Архивировать кампании |
| `direct_unarchive_campaigns` | Разархивировать кампании |
| `direct_delete_campaigns` | Удалить кампании |
| `direct_get_adgroups` | Получить группы объявлений |
| `direct_create_adgroup` | Создать группу объявлений |
| `direct_update_adgroup` | Обновить группу объявлений |
| `direct_get_ads` | Получить объявления |
| `direct_create_text_ad` | Создать текстовое объявление |
| `direct_update_ad` | Обновить объявление |
| `direct_moderate_ads` | Отправить на модерацию |
| `direct_suspend_ads` | Приостановить объявления |
| `direct_resume_ads` | Возобновить объявления |
| `direct_archive_ads` | Архивировать объявления |
| `direct_unarchive_ads` | Разархивировать объявления |
| `direct_delete_ads` | Удалить объявления |
| `direct_get_keywords` | Получить ключевые слова |
| `direct_add_keywords` | Добавить ключевые слова |
| `direct_delete_keywords` | Удалить ключевые слова |
| `direct_set_keyword_bids` | Установить ставки |
| `direct_get_statistics` | Получить статистику |

### Yandex Metrika

| Инструмент | Описание |
|------------|----------|
| `metrika_get_counters` | Получить список счётчиков |
| `metrika_get_counter` | Детали счётчика |
| `metrika_create_counter` | Создать счётчик |
| `metrika_delete_counter` | Удалить счётчик |
| `metrika_get_goals` | Получить цели |
| `metrika_create_goal` | Создать цель |
| `metrika_get_report` | Получить отчёт |
| `metrika_get_report_by_time` | Отчёт по времени |

## Примеры использования

### Управление кампаниями
```
Покажи все активные кампании
Приостанови кампанию "Летняя акция"
Увеличь дневной бюджет кампании 123 до 5000 рублей
```

### Работа с ключевыми словами
```
Покажи ключевые слова в группе 456
Добавь ключи "купить айфон" и "айфон цена" в группу 456
Установи ставку 50 рублей на ключ 789
```

### Аналитика
```
Покажи статистику сайта за последнюю неделю
Сколько было конверсий по цели "Заявка" за месяц?
Покажи источники трафика для счётчика 12345
```

### Создание объявлений
```
Создай текстовое объявление в группе 456:
- Заголовок: Купить iPhone 15
- Текст: Лучшие цены! Доставка бесплатно
- Ссылка: https://example.com/iphone
```

## Альтернативные способы запуска

### Напрямую через Python
```bash
python -m yandex_mcp
```

### Проверка через MCP Inspector
```bash
# Если установлен как пакет
npx @modelcontextprotocol/inspector yandex-mcp

# Или напрямую из папки проекта
npx @modelcontextprotocol/inspector python yandex_mcp.py
```

### Cursor IDE
Добавьте в настройки Cursor аналогично Claude Code.

## Безопасность

- Храните токены в переменных окружения, не в коде
- Используйте минимально необходимые права доступа
- Для тестирования используйте песочницу (`YANDEX_USE_SANDBOX=true`)
- Не коммитьте `.env` файлы

## Разработка

```bash
# Клонировать репозиторий
git clone https://github.com/SvechaPVL/yandex-mcp.git
cd yandex-mcp

# Установить зависимости для разработки
pip install -e ".[dev]"

# Запустить линтер
ruff check .

# Запустить тесты
pytest
```

## Лицензия

MIT License - используйте свободно.

## Ссылки

- [Документация Yandex Direct API](https://yandex.ru/dev/direct/doc/dg/concepts/about.html)
- [Документация Yandex Metrika API](https://yandex.ru/dev/metrika/doc/api2/concept/about.html)
- [Model Context Protocol](https://modelcontextprotocol.io/)

---

Сделано с ❤️ для автоматизации рекламы
