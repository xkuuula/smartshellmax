# SmartShell to MAX notifications

Async Python 3.12+ service that receives new club events directly from the
official SmartShell GraphQL API and sends matching notifications to a MAX chat
or group through the official MAX Bot API.

Telegram is not used at any stage.

## How It Works

The service:

- authenticates in SmartShell with the official `login` GraphQL mutation;
- polls the official `eventList` GraphQL query;
- stores seen SmartShell event IDs and an outgoing MAX queue in SQLite;
- sends only new matching events to MAX;
- retries temporary SmartShell and MAX failures with exponential backoff;
- does not send the same event again after restart.

The outgoing MAX message is formatted as:

```text
Клуб: 2803 | JustPlay

12:15 22.07.2026
Бонусное зачисление №138386080, на сумму 2000.00 ₽, клиенту FlixRey (+79110177855)
Комментарий: Новенький
```

The event body is taken from SmartShell `event.description`.

Shift closing events are handled specially. `eventList` contains only a short
`WORK_SHIFT_FINISHED` notification, so the service uses the event's
`work_shift.id` and then calls `getWorkShiftPaymentOverviewData(id: ...)` to
build the full shift summary.

## SmartShell API

The public SmartShell API documentation describes:

- GraphQL API access;
- `login` mutation for authorization;
- `eventList` query for club events;
- API rate limit of 20 requests per minute.

References:

- SmartShell API overview: https://apidoc.smartshell.gg/
- SmartShell authorization: https://apidoc.smartshell.gg/auth.html
- SmartShell event list: https://apidoc.smartshell.gg/eventList.html
- SmartShell shift payment overview: https://apidoc.smartshell.gg/getWorkShiftPaymentOverviewData.html
- SmartShell limits: https://apidoc.smartshell.gg/limits.html
- MAX send message: https://dev.max.ru/docs-api/methods/POST/messages

The commonly used GraphQL endpoint is:

```env
SMARTSHELL_API_URL=https://billing.smartshell.gg/api/graphql
```

If SmartShell changes the endpoint or your account uses another panel domain,
verify it in your own authorized SmartShell panel request in browser DevTools.

Do not bypass CAPTCHA, 2FA, access controls, or use leaked tokens. Use your own
SmartShell account credentials, preferably a dedicated integration account.

## Files

```text
main.py
config.py
smartshell_client.py
max_client.py
storage.py
logger.py
.env.example
requirements.txt
Dockerfile
.dockerignore
data/.gitkeep
README.md
```

## Setup

Create and activate a virtual environment:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and set real values:

```env
SMARTSHELL_API_URL=https://billing.smartshell.gg/api/graphql
SMARTSHELL_LOGIN=your_smartshell_login
SMARTSHELL_PASSWORD=your_smartshell_password
SMARTSHELL_COMPANY_ID=2803
SMARTSHELL_CLUB_TITLE=JustPlay

MAX_BOT_TOKEN=your_real_max_bot_token
MAX_TARGET_CHAT_ID=-77153443244225
DATABASE_PATH=data/smartshell_max.db
```

Run:

```powershell
python main.py
```

On hosting, store all real values as environment variables or create `.env`
manually on the server after cloning. Never commit `.env`.

## Filtering

By default, the service forwards events whose SmartShell description contains
one of these fragments:

```env
SMARTSHELL_DESCRIPTION_KEYWORDS=режим высокого доступа,восстановлена работа,Бонусное зачисление,Изменена конфигурация оборудования,списал товары со склада,оприходовал товары на склад,Новая конфигурация оборудования,расходный кассовый ордер,приходный кассовый ордер,Использована скидка,деактивирован шелл,начал рабочую смену,Возврат платежа,заканчивается,больше нет на складе,Новый отзыв о клубе,изменил депозит клиента,изменил бонусный счет клиента,изменил скидку клиента,не ответил на вызов
```

To forward everything returned by SmartShell:

```env
SMARTSHELL_SEND_ALL_EVENTS=true
SMARTSHELL_DESCRIPTION_KEYWORDS=
SMARTSHELL_EVENT_TYPES=
```

To filter by SmartShell event types, set comma-separated API event type names:

```env
SMARTSHELL_EVENT_TYPES=PAYMENT,CLIENT_BALANCE
```

Use type names exactly as returned by your SmartShell API.

## Reliability

On the first launch, the service starts from the current time and does not send
old history. On later restarts, it queries a configurable look-back window:

```env
SMARTSHELL_POLL_WINDOW_MINUTES=120
SMARTSHELL_WAREHOUSE_POLL_INTERVAL_SECONDS=300
```

SQLite keeps:

- the last SmartShell event cursor;
- the last SmartShell warehouse operation cursor;
- all seen SmartShell event IDs;
- outgoing MAX messages and their delivery status.

Do not delete `smartshell_max.db` unless you intentionally want to reset duplicate
protection.

The service requests up to `SMARTSHELL_MAX_PAGES_PER_POLL` event pages per cycle.
Warehouse stock operations are polled separately through `goods` and
`goodHistory`. With 34 goods and
`SMARTSHELL_WAREHOUSE_POLL_INTERVAL_SECONDS=300`, warehouse polling adds about
7 requests per minute and stays within the documented SmartShell limit together
with the regular event polling.

## GitHub Safety

Commit `.env.example`, not `.env`.

Do not commit:

```text
.env
smartshell_max.db
service.log
.venv/
__pycache__/
```

If credentials were exposed, rotate them before deployment.
