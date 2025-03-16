import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests

from sql_handler import read_sql_market_stats, create_engine
from get_jita_prices import get_jita_prices
from MarketStructures8 import fetch_market_orders

sde = r"sqlite:///C:/Users/User/PycharmProjects/ESI_Utilities/SDE/SDE sqlite-latest.sqlite"

def get_ships_price_analysis()->pd.DataFrame:
    df = read_sql_market_stats()
    ships = df[df.category_name == 'Ship']
    ships2 = get_jita_prices(ships)
    pd.set_option('display.max_columns', None)
    ships2['delta'] = ships2['jita_sell'] - ships2['avg_of_avg_price']
    ships2['%delta'] = ships2['delta'] / ships2['avg_of_avg_price'] * 100
    ships2['jita_var'] = ships2['delta'] * ships2['avg_daily_volume']
    return ships2

def get_BP_ids(df:pd.DataFrame)->pd.DataFrame:
    engine = create_engine(sde)
    with engine.connect() as conn:
        bp_df = pd.read_sql_table('industryActivityProducts',conn)
    bp_df = bp_df[bp_df.productTypeID.isin(df['type_id'])]
    bp_df.rename(columns={'productTypeID':'type_id', 'typeID':'bp_id'}, inplace=True)
    return bp_df

def get_build_cost(bp_ids:list)->pd.DataFrame:
    id_count = len(bp_ids)
    id_split = round(id_count/10,-1)

    total_cost = []
    bp_type_id = []
    bp_name = []
    bp_cost_df = pd.DataFrame()

    for id in bp_ids:
        id_str = str(id)
        print(id_str)

        cookbook_url = f"https://evecookbook.com/api/buildCost?blueprintTypeId={id_str}&quantity=1&priceMode=buy&additionalCosts=0&baseMe=0&componentsMe=10&system=MC6O-F&facilityTax=1&industryStructureType=Raitaru&industryRig=T2&reactionStructureType=Tatara&reactionRig=T2&reactionFlag=Yes&blueprintVersion=tq"

        response = requests.get(cookbook_url)
        record = response.json()
        total_cost.append(record['message']['totalCost'])
        bp_type_id.append(record['message']['blueprintTypeId'])
        bp_name.append(record['message']['blueprintName'])

    bp_cost_df['total_cost'] = total_cost
    bp_cost_df['bp_id'] = bp_type_id

    df3 = bp_cost_df.merge(bp_cost_df, on='bp_id')
    return df3

def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    pass