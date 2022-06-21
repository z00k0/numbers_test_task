from datetime import datetime
import gspread
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

auth_json = os.getenv('AUTH_JSON')
spreadsheet_id = os.getenv('SPREADSHEET_ID')
currency_id = 'R01235'


def get_data_from_gsheet(sheet_id, auth_json):
    gc = gspread.service_account(filename=auth_json)
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.get_worksheet(0)
    # heads_list = worksheet.row_values(1)
    records_dict = worksheet.get_all_records()
    # records_list = worksheet.get_all_values()
    # print(records_list)
    for row in records_dict:
        row['срок поставки'] = datetime.strptime(row['срок поставки'], '%d.%m.%Y').date()
    return records_dict


# sheet_data = get_data_from_gsheet(sheet_id=spreadsheet_id, auth_json=auth_json)
# print(sheet_data)
# get_data_from_gsheet(spreadsheet_id, auth_json)

def get_df_from_gsheet(sheet_id, auth_json):
    gc = gspread.service_account(filename=auth_json)
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.get_worksheet(0)
    df = pd.DataFrame(worksheet.get_all_records())
    df.rename(
        columns={'№': 'number', 'заказ №': 'order_id', 'стоимость,$': 'price_usd', 'срок поставки': 'delivery_time'},
        inplace=True)
    df['delivery_time'] = pd.to_datetime(df['delivery_time'], format='%d.%m.%Y')
    return df

# d = get_df_from_gsheet(spreadsheet_id, auth_json)
# print(d)
