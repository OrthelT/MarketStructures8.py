import pandas as pd
import requests

# Tools to retrieve Jita prices using the Fuzzworks market API

region_id = '10000002'
type_list = pd.read_csv('data/type_ids.csv')
id_list = type_list['type_ids'].to_list()
id_list = ','.join(map(str, id_list))


def get_jita_prices(regionid, ids):
    base_url = 'https://market.fuzzwork.co.uk/aggregates/?region='
    url = f'{base_url}{regionid}&types={ids}'

    response = requests.get(url)
    data = response.json()
    parsedata = parse_json(data)
    return parsedata


def parse_json(data):
    # Prepare data for DataFrame
    rows = []
    for item_id, item_data in data.items():
        buy_data = item_data.get("buy", {})
        sell_data = item_data.get("sell", {})
        rows.append({
            "item_id": item_id,
            "buy_weighted_avg": float(buy_data.get("weightedAverage", 0)),
            "buy_max": float(buy_data.get("max", 0)),
            "sell_weighted_avg": float(sell_data.get("weightedAverage", 0)),
            "sell_max": float(sell_data.get("max", 0)),
        })
    df = pd.DataFrame(rows)
    return df
