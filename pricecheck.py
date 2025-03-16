import sys

import pandas as pd
from sqlalchemy import create_engine, text
from get_jita_prices import get_jita_sell

mkt_sqlfile = "sqlite:///market_orders.sqlite"

def get_item_info(name):
    engine = create_engine(mkt_sqlfile, echo=False)
    stmt = "SELECT * FROM main.Market_Stats WHERE Market_Stats.type_name LIKE :name"
    params = {'name': f'%{name}%'}
    engine = create_engine(mkt_sqlfile, echo=False)

    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn, params=params)
        df3 = pd.read_sql("SELECT * FROM main.Doctrines WHERE Doctrines.ship LIKE :name", conn, params=params)
    df2 = get_jita_sell(df)
    sell_formatted = "{:,.0f}".format(df.price_5th_percentile.unique()[0])
    avg_formatted = "{:,.0f}".format(df.avg_of_avg_price.unique()[0])
    jita_sell_formatted = "{:,.0f}".format(df2.jita_sell.unique()[0])
    ship_name = df.type_name.unique()[0]
    jita_sell = df2.jita_sell.unique()[0]
    four_h_sell = df.price_5th_percentile.unique()[0]
    hulls = df.total_volume_remain.unique()[0]
    percent_diff = (four_h_sell-jita_sell) / jita_sell * 100
    formatted_diff = "{:,.1f}".format(percent_diff)
    df4 = df3[df3.ship == ship_name]

    print(f'''
    type_name: {df.type_name.unique()[0].upper()}
    ==============================
    type_id: {df.type_id.unique()[0]}
    -----------------------------
    stock: {df.total_volume_remain.unique()[0]}
    fits = {int(df3.fits.min())}
    days_remaining: {df.days_remaining.unique()[0]}
    avg_volume: { round( df.avg_daily_volume.unique()[0],1)}
    sell_price: {sell_formatted}
    avg_price: {avg_formatted}
    -----------------------------
    jita_sell: {jita_sell_formatted}
    pct_diff: {formatted_diff}%
    -----------------------------
    ''')

    pd.set_option('display.max_columns', None)
    cols = ['type_id', 'item', 'stock',
            'fits', 'days', 'price_4h']

    df4 = df3[df3.fits < hulls].reset_index(drop=True)
    if df4.empty:
        pass
    else:
        df5 = df4.drop_duplicates(subset=['type_id', 'ship'], keep='first')
        df6 = df5[cols]
        print(df6)

    print(f"""
    -----------------------------------------------
    MARKET QUERY COMPLETE
    updated: {df.timestamp.unique()[0]}
    """)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: prchk <item_name>")
        sys.exit(1)

    item_name = sys.argv[1]
    get_item_info(item_name)
