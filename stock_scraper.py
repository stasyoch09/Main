"""
Скрипт для копирования котировок акций Московской биржи в Google Таблицу.
Авторизация Google: через сервисный аккаунт (JSON-ключ передаётся через
переменную окружения GOOGLE_SERVICE_ACCOUNT_JSON).

Зависимости:
    pip install requests gspread google-auth
"""

import os
import json
import logging
import datetime

import requests
import gspread
from google.oauth2.service_account import Credentials

# ─── Настройки ────────────────────────────────────────────────────────────────

SPREADSHEET_ID = "119uhDl0TqA3C3rVoCBgUpj2X_HMDfYNEQKHsXg0ABN0"
SHEET_NAME     = "Котировки"

MOEX_URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
    "?iss.meta=off&iss.only=securities,marketdata"
    "&securities.columns=SECID,SHORTNAME,PREVPRICE"
    "&marketdata.columns=SECID,LAST,CHANGE,LASTTOPREVPRICE,VALTODAY,MARKETPRICE2"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADER = ["Тикер", "Название", "Цена посл.", "Изм. руб.", "Изм. %", "Объём руб.", "Рын. цена"]

# ─── Логирование ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ─── Авторизация через сервисный аккаунт ─────────────────────────────────────

def get_worksheet() -> gspread.Worksheet:
    service_account_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not service_account_json:
        raise EnvironmentError("Переменная окружения GOOGLE_SERVICE_ACCOUNT_JSON не задана!")

    service_account_info = json.loads(service_account_json)
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=10000, cols=10)
        log.info("Создан новый лист «%s»", SHEET_NAME)

    return worksheet


# ─── Получение котировок с MOEX ISS API ───────────────────────────────────────

def fetch_quotes() -> list[list]:
    log.info("Запрашиваем котировки с MOEX ISS API...")
    resp = requests.get(MOEX_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    sec_cols = data["securities"]["columns"]
    sec_rows = data["securities"]["data"]
    md_cols  = data["marketdata"]["columns"]
    md_rows  = data["marketdata"]["data"]

    md_dict = {row[md_cols.index("SECID")]: row for row in md_rows}

    rows = []
    for sec in sec_rows:
        secid     = sec[sec_cols.index("SECID")]
        shortname = sec[sec_cols.index("SHORTNAME")]
        prevprice = sec[sec_cols.index("PREVPRICE")]

        md = md_dict.get(secid)
        if md is None:
            continue

        last      = md[md_cols.index("LAST")]
        change    = md[md_cols.index("CHANGE")]
        changepct = md[md_cols.index("LASTTOPREVPRICE")]
        valtoday  = md[md_cols.index("VALTODAY")]
        mktprice  = md[md_cols.index("MARKETPRICE2")]

        if last is None and prevprice is None:
            continue

        rows.append([
            secid,
            shortname,
            last if last is not None else prevprice,
            change    if change    is not None else "",
            changepct if changepct is not None else "",
            valtoday  if valtoday  is not None else "",
            mktprice  if mktprice  is not None else "",
        ])

    log.info("Получено %d бумаг", len(rows))
    return rows


# ─── Запись в Google Sheets ───────────────────────────────────────────────────

def write_to_sheet(worksheet: gspread.Worksheet, quotes: list[list]) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    worksheet.clear()
    log.info("Лист очищен.")

    data_to_write = [[f"Обновлено: {now}"], HEADER] + quotes

    worksheet.update(
        "A1",
        data_to_write,
        value_input_option="USER_ENTERED",
    )
    log.info("Zapisano %d строк.", len(data_to_write))


# ─── Точка входа ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    worksheet = get_worksheet()
    quotes = fetch_quotes()
    write_to_sheet(worksheet, quotes)
    log.info("✅ Готово.")
