import json

import pandas as pd
import pan_db_handler as pdb


def aggregate_orders(orders: pd.DataFrame, ids: list[int]) -> pd.DataFrame:
    print("Aggregating orders...")
    filtered_orders = orders[orders['type_id'].isin(ids)]
    sell_orders = filtered_orders[filtered_orders['is_buy_order'] == False]

    grouped_df = sell_orders.groupby('type_id')['volume_remain'].sum().reset_index()
    grouped_df.columns = ['type_id', 'total_volume_remain']

    min_price_df = sell_orders.groupby('type_id')['price'].min().reset_index()
    min_price_df.columns = ['type_id', 'min_price']

    percentile_5th_df = sell_orders.groupby('type_id')['price'].quantile(0.05).reset_index()
    percentile_5th_df.columns = ['type_id', 'price_5th_percentile']

    merged_df = pd.merge(grouped_df, min_price_df, on='type_id')
    merged_df = pd.merge(merged_df, percentile_5th_df, on='type_id')

    return merged_df


def update_doctrines():
    doctrine_ids = pd.read_csv('data/watchlist_update/FNI_CFI_TypeIDs.csv')
    ids = doctrine_ids['TYPE_ID'].tolist()
    orders = pdb.read_market_orders()
    orders['type_id'] = pd.to_numeric(orders['type_id'], errors='coerce')
    df = aggregate_orders(orders, ids)
    df.to_csv('data/watchlist_update/FNI_CFI(UPDATE).csv', index=False)
    return df


if __name__ == "__main__":
    pass

    # new_cols = ['type_id', 'v_dps', 'v_lin', 'clay','FNI', 'CFI', 'Basi']
    # old_cols = doctrine_ids.columns
    #
