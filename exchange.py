import logging
from xml.etree import ElementTree as etree

import requests


logger = logging.getLogger(__name__)

currency_id = 'R01235'


def get_exchange_rate(currency_id, date):
    date = date
    day, month, year = date.day, date.month, date.year
    url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={day:02d}/{month:02d}/{year}'
    session = requests.Session()
    resp = session.get(url)
    if resp.status_code == 200:
        tree = etree.fromstring(resp.content)
        usd = tree.findall(f'./Valute[@ID="{currency_id}"]/Value')[0].text
        usd = float(usd.replace(',', '.'))
        return usd
    else:
        return resp.status_code

