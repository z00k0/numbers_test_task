import logging
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

metadata_obj = sa.MetaData()

orders = sa.Table(
    'orders', metadata_obj,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
    sa.Column('number', sa.Integer),
    sa.Column('order_id', sa.Integer),
    sa.Column('price_usd', sa.Float),
    sa.Column('delivery_time', sa.Date),
    sa.Column('price_rub', sa.Float)
)

overdue_orders = sa.Table(
    'overdue_orders', metadata_obj,
    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
    sa.Column('order_id', sa.Integer, sa.ForeignKey('orders.id', ondelete='SET NULL'), nullable=True)
)


def create_tables(engine):
    metadata_obj.create_all(engine)


def get_df_from_db(conn):
    df = pd.read_sql('select * from "orders"', conn)
    df['delivery_time'] = pd.to_datetime(df['delivery_time'], format='%Y-%m-%d')
    return df


def delete_records(conn, dataframe: pd.DataFrame):
    ids_to_delete = dataframe['id'].to_list()
    delete = orders.delete().where(orders.c.id.in_(ids_to_delete))
    conn.execute(delete)


def get_overdue_records(conn, date):
    exist = sa.exists().where(orders.c.id == overdue_orders.c.order_id)
    select = orders.select().filter(~exist).filter(orders.c.delivery_time < date)
    result = conn.execute(select)
    records = result.fetchall()
    return records


def insert_sent_record(conn, record):
    ins = overdue_orders.insert().values(order_id=record[0])
    conn.execute(ins)
    logging.info(f'inserted {record}')
