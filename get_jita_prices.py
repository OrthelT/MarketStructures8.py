import json

import pandas as pd
import requests

# Tools to retrieve Jita prices using the Fuzzworks market API
# Sample data to use for testing
file = 'data/mining_basket.csv'
df = pd.read_csv(file)
basket_ids = df.type_id.to_list()
ids = basket_ids[:6]
ids = [int(x) for x in ids]


def get_jita_prices(vale_data):
    regionid = '10000002'
    base_url = 'https://market.fuzzwork.co.uk/aggregates/?region='
    ids = vale_data['type_id'].to_list()
    ids_str = ','.join(map(str, ids))
    url = f'{base_url}{regionid}&types={ids_str}'
    response = requests.get(url)
    data = response.json()
    jita_data = parse_json(data)
    merged_df = merge_vale_data(jita_data, vale_data)
    return merged_df

def merge_vale_data(jita_data, vale_data):
    vale_df = vale_data.copy()
    vale_df['type_id'] = vale_df['type_id'].astype(int)
    jita_data.columns = ['type_id', 'jita_sell', 'jita_buy']
    jita_data['type_id'] = jita_data['type_id'].astype(int)
    merged_df = pd.merge(vale_df, jita_data, on='type_id', how='left')
    merged_df = merged_df.reset_index(drop=True)
    merged_df = merged_df[['type_id', 'type_name', 'total_volume_remain', 'price_5th_percentile',
                           'avg_of_avg_price', 'avg_daily_volume', 'group_id', 'jita_sell', 'jita_buy',
                           'group_name', 'category_id', 'category_name']]
    return merged_df

def parse_json(data) -> pd.DataFrame:
    # Prepare data for DataFrame
    rows = []
    for item_id, item_data in data.items():

        buy_data = item_data.get("buy", {})
        sell_data = item_data.get("sell", {})

        rows.append({
            "type_id": item_id,
            "jita_sell": float(sell_data.get("percentile")),
            "jita_buy": float(buy_data.get("percentile")),
        })

    df = pd.DataFrame(rows)
    df['jita_sell'] = df['jita_sell'].round(2)
    df['jita_buy'] = df['jita_buy'].round(2)

    return df


def get_jita_history(type_ids: list):
    start_date = '2024-01-01'
    end_date = '2024-12-29'

    base_url = 'https://api.adam4eve.eu/v1/market_price_history?typeID='
    start = '&start=' + start_date
    end = '&end=' + end_date
    types_url = base_url + ','.join(map(str, type_ids))
    url = types_url + start + end

    response = requests.get(url)
    data = response.json()

    with open('data/market_basket.json', 'w') as f:
        json.dump(data, f)

    df = pd.DataFrame(data)

    price_columns = [
        "buy_price_low", "buy_price_avg", "buy_price_high",
        "sell_price_low", "sell_price_avg", "sell_price_high"
    ]
    volume_columns = [
        "buy_volume_low", "buy_volume_avg", "buy_volume_high",
        "sell_volume_low", "sell_volume_avg", "sell_volume_high"
    ]
    df[volume_columns] = df[volume_columns].astype(int)
    df[price_columns] = df[price_columns].astype(float).round(2)

    df.to_csv('data/mining_basket_history.csv', index=False)

    return df


def process_market_basket():
    df1 = pd.read_csv('data/mining_basket.csv')
    df2 = pd.read_csv('data/mining_basket_history.csv')
    df1 = df1.infer_objects()
    df1.dropna(inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df1['type_id'] = df1['type_id'].astype(int)
    df1.rename(columns={'Ore': 'ore', 'Qty': 'qty'}, inplace=True)
    print(df1.head())
    df1['ore'] = df1['ore'].astype(str)
    # df1['qty'] = df1['qty'].apply(lambda x: int(x.strip().replace(',', '')))
    df3 = df2.merge(df1, on='type_id')
    df3['price_date'] = pd.to_datetime(df3['price_date'])
    df3['ore'] = df3['ore'].str.lower()
    print(df3.columns)
    df3['mined_value'] = df3.apply(lambda row: row['sell_price_avg'] * row['qty'], axis=1)
    df3 = df3[['price_date', 'ore', 'sell_price_avg', 'mined_value']]
    df3.to_csv('data/mined_items_by_day.csv', index=False)
    df = df3.copy()
    df_grouped = df.groupby('price_date').mined_value.sum()
    df_grouped = df_grouped.reset_index()
    print(df_grouped.head())
    df_grouped.to_csv('data/mined_value_by_day.csv', index=False)
    return df_grouped

if __name__ == "__main__":
    process_market_basket()
