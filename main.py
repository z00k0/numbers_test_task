import datetime
import os
import time
import logging
from logging import FileHandler, Formatter

import sqlalchemy as sa
from dotenv import load_dotenv

from db import delete_records, get_df_from_db, metadata_obj, get_overdue_records, insert_sent_record, create_tables
from exchange import get_exchange_rate
from sheets import get_df_from_gsheet
from telegram_sender import send_message

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = FileHandler('main.log', encoding='utf-8')
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

auth_json = os.getenv('AUTH_JSON')  # путь к файлу с токеном для доступа к google API
spreadsheet_id = os.getenv('SPREADSHEET_ID')  # id гугл таблицы
currency_id = 'R01235'  # id валюты на сайте cbr.ru, для USD - 'R01235'
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_SERVER = os.getenv('POSTGRES_SERVER')

DATABASE_URL = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:5432/{POSTGRES_DB}'

engine = sa.create_engine(DATABASE_URL, client_encoding='utf8')
conn = engine.connect()

create_tables(engine)

while True:
    usd_exchange_rate = get_exchange_rate(currency_id=currency_id, date=datetime.datetime.now())
    logger.info(f'{usd_exchange_rate=}')
    gsheet_df = get_df_from_gsheet(sheet_id=spreadsheet_id, auth_json=auth_json)
    db_df = get_df_from_db(conn=conn)
    df_to_insert = gsheet_df.merge(db_df, how='left', indicator=True)
    df_to_insert = df_to_insert[(df_to_insert['_merge'] == 'left_only')]
    df_to_insert.drop(columns='_merge', inplace=True)

    df_to_delete = gsheet_df.merge(db_df, how='right', indicator=True)
    df_to_delete = df_to_delete[(df_to_delete['_merge'] == 'right_only')]
    df_to_delete.drop(columns='_merge', inplace=True)
    if len(df_to_delete) > 0:
        logger.info(f'Deleting {len(df_to_delete)} records')
        delete_records(conn, df_to_delete)

    if len(df_to_insert) > 0:
        df_to_insert.drop(columns='id', inplace=True)
        df_to_insert['price_rub'] = round(df_to_insert['price_usd'] * usd_exchange_rate, 2)
        logger.info(f'Updating/inserting {len(df_to_insert)} records')
        df_to_insert.to_sql('orders', con=conn, if_exists='append', index=False)

    overdue = get_overdue_records(conn, date=datetime.datetime.now())
    if overdue:
        logger.info(f'messages to sent: {len(overdue)}')
        for record in overdue:
            result = send_message(record)
            if result.get('ok'):  # если сообщение отправилось
                logger.info(f'message sent for заказ №: {record[1]}')
                insert_sent_record(conn, record)  # добавляется запись в бд
            time.sleep(1)

    time.sleep(60)  # Проверка новых записей в гугл таблице 1 раз в минуту. Можно поставить интервал проверки меньше.
                    # но надо иметь ввиду, гугл ограничивает количество запросов не более 60 в минуту.
