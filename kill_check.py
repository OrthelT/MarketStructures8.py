import json
from datetime import datetime, timezone
from logging import getLogger

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine

from sql_handler import insert_pd_type_names, get_doctrine_status_optimized

logger = getLogger('mkt_structures.kill_check')

data_loc = 'data/'
frt = 99003581
cat = 'killmail'


def frt_km_query():
    logger.info("initiating killmail query")

    url = "https://eve-kill.com/api/query"
    headers = {
        "Content-Type": "application/json"
    }

    logger.info("dropping payload")
    payload = {
        "type": "complex",
        "filter": {
            "victim.alliance_id": 99003581
        },
        "options": {
            "limit": 1000,
            "skip": 2000,
            "projection": {
                "killmail_id": 1,
                "kill_time": 1,
                "victim.character_id": 1,
                "victim.ship_id": 1,
                "victim.ship_group_id": 1
            }
        }
    }

    logger.info('sending request')
    response = requests.post(url, headers=headers, json=payload)
    status = response.headers
    print(status)

    # Check the response
    if response.status_code == 200:
        logger.info('response received with status code 200')
        # Save JSON to a file
        logger.info('saving response to file')
        with open(f"data/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}", "w") as f:
            json.dump(response.json(), f, indent=4)
        logger.info('response saved to file')

    else:
        logger.error(f"Failed to fetch data. Status code: {response.status_code}")
        logger.error("Response:", response.text)

    data = response.json()
    logger.info('returning data')
    return data


def get_ship_loss_stats():
    # DO NOT FORGET TO REMOVE THIS GARBAGE
    test_mode = False
    if test_mode:
        with open('data/2025-01-03_00-27-14.json') as f:
            data = json.load(f)
        logger.info('running get ship_loss_stats() in test mode')
    else:
        data = frt_km_query()
        logger.info('running get ship_loss_stats() in live mode')
    # Refactor the above

    flat_data = []

    for item in data:
        km_id = item['killmail_id']
        k_time = item['kill_time']

        vic = item['victim']
        type_id = vic['ship_id']
        group_id = vic['ship_group_id']
        char_id = vic['character_id']

        flat_data.append([type_id, group_id, k_time, char_id, km_id])

    df = pd.DataFrame(flat_data, columns=['type_id', 'group_id', 'kill_time', 'character_id', 'killmail_id'])
    df['kill_time'] = pd.to_datetime(df['kill_time'], unit='s')
    df_named = insert_pd_type_names(df)

    ts = datetime.now(tz=timezone.utc)
    df_named['timestamp'] = ts
    df_named['timestamp'] = pd.to_datetime(df_named['timestamp'])

    df = get_doctrine_status_optimized(
    )

    print(df.head())
    engine = create_engine(f'{testdb}')
    with engine.connect() as conn:
        df.to_sql('Doctrines', conn, if_exists='replace', index=False)

    return df_named


if __name__ == '__main__':
    with open('data/2025-01-03_00-27-14.json') as f:
        data = json.load(f)
    dates = []
    real_dates = []
    for item in data:
        dates.append(item['kill_time'])
        real_dates.append(datetime.fromtimestamp(item['kill_time']))

    dy = datetime.today()

    yd = dy + relativedelta(days=-7)

    print(yd)
    # "killmail_id": 123668120,
    # "kill_time": 1735753973,
    # "victim": {
    #     "ship_id": 11379,
    #     "ship_group_id": 324,
    #     "character_id": 2122430704
