import logging

from dotenv import load_dotenv
import requests
import os

load_dotenv()
logger = logging.getLogger(__name__)
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
channel_id = os.getenv('channel_id')


def send_message(text):
    url = 'https://api.telegram.org/bot' + TELEGRAM_TOKEN + '/sendMessage'
    _id, number, order_id, price_usd, delivery_time, price_rub = text
    card = f'!!! Вышел срок доставки !!!\n' \
           f'№: {number}\n' \
           f'заказ №: {order_id}\n' \
           f'стоимость,$: {price_usd}\n' \
           f'срок поставки: {delivery_time}\n' \
           f'стоимость, руб: {price_rub}'
    resp = requests.post(url, data={
        "chat_id": channel_id,
        "text": card
    })
    return resp.json()


if __name__ == '__main__':
    send_message("test message")
