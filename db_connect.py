import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from local_settings import postgresql as settings

postgresql = {'pguser':'bjagrelli',
              'pgpasswd':'25121944',
              'pghost':'localhost',
              'pgport': 5432,
              'pgdb': 'olist_db'
             }

def get_engine(user, passwd, host, port, db):
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)
    return engine

def get_engine_from_settings():
    keys = ['pguser','pgpasswd','pghost','pgport','pgdb']
    if not all(key in keys for key in settings.keys()):
        raise Exception('Bad config file')

    return get_engine(settings['pguser'],
                      settings['pgpasswd'],
                      settings['pghost'],
                      settings['pgport'],
                      settings['pgdb'])

def create_schema(schema):
    con = engine.connect()
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    con.close()


# DATA CLEANSING E FEATURE ENGINEERING

def data_cleansing():
    customers['customer_city'] = customers['customer_city'].str.upper()

    orders['order_delivery_time'] = ((orders['order_delivered_customer_date'] - orders['order_purchase_timestamp'])/np.timedelta64(1, 'h')).round(2)
    orders['delivered_in_time'] = orders.apply(lambda row: True if row['order_delivered_customer_date'] < row['order_estimated_delivery_date'] else False, axis=1)
    orders.drop(orders[(orders['order_status'] == 'unavailable') | (orders['order_status'] == 'canceled')].index, inplace=True)

    order_items['total_price'] = order_items['price'] + order_items['freight_value']

if __name__ == '__main__':
    schema = 'raw'
    engine = get_engine_from_settings()
    create_schema(schema)
    
    # LEITURA DOS ARQUIVOS

    customers = pd.read_csv('data/olist_customers_dataset.csv')
    orders = pd.read_csv('data/olist_orders_dataset.csv', parse_dates = [3,4,5,6,7], infer_datetime_format=True)
    order_items = pd.read_csv('data/olist_order_items_dataset.csv')
    order_payments = pd.read_csv('data/olist_order_payments_dataset.csv')
    order_reviews = pd.read_csv('data/olist_order_reviews_dataset.csv')
    products = pd.read_csv('data/olist_products_dataset.csv')
    sellers = pd.read_csv('data/olist_sellers_dataset.csv')
    product_category_name = pd.read_csv('data/product_category_name_translation.csv')

    # IMPORTAÇÃO DOS DADOS PARA TABELAS NO BANCO

    customers.to_sql('customers', con=engine, schema='raw', index=False, if_exists='replace')
    order_items.to_sql('order_items', con=engine, schema='raw', index=False, if_exists='replace')
    order_payments.to_sql('order_payments', con=engine, schema='raw', index=False, if_exists='replace')
    order_reviews.to_sql('order_reviews', con=engine, schema='raw', index=False, if_exists='replace')
    orders.to_sql('orders', con=engine, schema='raw', index=False, if_exists='replace')
    products.to_sql('products', con=engine, schema='raw', index=False, if_exists='replace')
    sellers.to_sql('sellers', con=engine, schema='raw', index=False, if_exists='replace')
    product_category_name.to_sql('product_category_name', con=engine, schema='raw', index=False, if_exists='replace')
