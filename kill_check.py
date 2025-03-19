import time
from datetime import datetime, timezone
from logging import Formatter, basicConfig, StreamHandler
from logging import getLogger, INFO, DEBUG, FileHandler

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

import sql_handler

logger = getLogger('kc_log')
logger.setLevel(INFO)

# File handler
file_handler = FileHandler('logs/kc_log.log')
file_handler.setLevel(DEBUG)
file_handler.setFormatter(
    Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

console_handler = StreamHandler()
console_handler.setFormatter(Formatter("%(name)s - %(levelname)s - %(message)s"))
console_handler.setLevel(INFO)
logger.addHandler(console_handler)

basicConfig(

    level=INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

data_loc = 'data/'
frt = 99003581
cat = 'killmail'
mkt_sqlfile = "sqlite:///market_orders.sqlite"


def frt_km_query(skip: int, date_after) -> dict or None:
    logger.info("initiating killmail query")

    url = "https://eve-kill.com/api/query"
    headers = {
        "Content-Type": "application/json"
    }
    print(date_after)
    logger.info("dropping payload")
    payload = {
        "type": "complex",
        "filter": {
            "victim.alliance_id": 99003581,
            "kill_time": {"$gte": 1735344000}
        },
        "options": {
            "limit": 1000,
            "skip": skip,
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
    logger.info(f'status: {status}')

    # Check the response
    if response.status_code != 200:
        logger.error(f"Failed to fetch data. Status code: {response.status_code}")
        logger.error("Response:", response.text)
    else:
        logger.info('response received with status code 200')

    data = response.json()
    logger.info('returning data')

    return data

def get_ship_loss_stats():
    losses = []
    today = datetime.today()
    week_ago = today + relativedelta(days=-7)
    week_ago_ts = week_ago.timestamp()
    kill_time = week_ago + relativedelta(minutes=+1)
    req_num = 0
    error_count = 0
    total_items = 0
    skip = 0

    while kill_time > week_ago:

        item_count = 0

        logger.info(f"""
        REQUEST#: {req_num}
        KILL TIME: {kill_time}
        """)

        req_num += 1
        time.sleep(.1)

        logger.info('starting request >> call frt_km_query()')
        data = frt_km_query(skip=skip, date_after=week_ago_ts)
        skip += 1000

        if data:
            print(type(data))
            for item in data:
                item_count += 1
                km_id = item['killmail_id']
                kill_time = datetime.fromtimestamp(item['kill_time'])
                vic = item['victim']
                type_id = vic['ship_id']
                group_id = vic['ship_group_id']
                char_id = vic['character_id']
                losses.append([km_id, kill_time, char_id, type_id, group_id, ])
                total_items += 1
            logger.info(f'items added: {item_count}')
            logger.info(f'total items: {total_items}')
            logger.info(f'killmail id: {km_id}')
            logger.info(f'kill time: {kill_time}')
            logger.info(f'character id: {char_id}')
            logger.info(f'type id: {type_id}')
            logger.info(f'group id: {group_id}')
            logger.info(f'error count: {error_count}')
            logger.info(f'request number: {req_num}')
            logger.info(f'skip: {skip}')
            logger.info(f'week ago: {week_ago}')

        elif error_count > 10:
            logger.error(f'error: too many errors, exiting')
            break
        else:
            error_count += 1
            logger.error(f'error: repeating request {req_num}')
            logger.info(f'error count: {error_count}')
            req_num = req_num - 1
            time.sleep(1)

    logger.info('ending request, returning data')
    logger.info(f'error count: {error_count}, ')

    df = pd.DataFrame(losses, columns=['killmail_id', 'kill_time', 'character_id', 'type_id', 'group_id'])
    # df['kill_time'] = pd.to_datetime(df['kill_time'], unit='s')

    ts = datetime.now(tz=timezone.utc)
    df['timestamp'] = ts
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.drop_duplicates(subset=['killmail_id'])
    df.reset_index(drop=True, inplace=True)

    save_kill_stats(df=df)

    return df


def save_kill_stats(df: pd.DataFrame) -> None:
    df = sql_handler.insert_pd_type_names(df=df)
    df.drop_duplicates(subset=['killmail_id'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    engine = create_engine(f'{mkt_sqlfile}')
    table = 'ShipsDestroyed'
    logger.info(f'saving to database: {table}')

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM Market_Stats"))
        conn.commit()
        df.to_sql(table, engine, if_exists='replace', index=False)

    df.to_csv('output/latest/recent_ship_losses.csv', index=False)
    logger.info(f'saved to database: {table}')
    return


def aggregate_kills() -> pd.DataFrame or None:
    engine = create_engine(f'{mkt_sqlfile}')
    table = 'ShipsDestroyed'

    with engine.connect() as conn:
        df = pd.read_sql(table, conn)

    kills_by_type = agg_kills_by_type(df=df)
    kills_by_day = agg_kills_by_day(df=df)
    total_kills_by_day = agg_total_kills_by_day(df=df)

    print('---------------------------------')
    print(f'killmail count: {len(df)}')
    print(df.head())
    print(kills_by_type.head())
    print(kills_by_day.head())
    print(total_kills_by_day.head())

    kills_by_type.to_csv('output/latest/aggregated_kills_by_type.csv', index=False)
    kills_by_day.to_csv('output/latest/aggregated_kills_by_day.csv', index=False)
    total_kills_by_day.to_csv('output/latest/aggregated_total_kills_by_day.csv', index=False)


def agg_kills_by_type(df: pd.DataFrame) -> pd.DataFrame:
    agg_kills_type = df.groupby(['type_id', 'type_name']).killmail_id.count().reset_index()
    agg_kills_type.rename(columns={'killmail_id': 'kill_count'}, inplace=True)
    return agg_kills_type


def agg_kills_by_day(df: pd.DataFrame) -> pd.DataFrame:
    agg_kills_day = df.groupby(['kill_time', 'type_id', 'type_name']).killmail_id.count().reset_index()
    agg_kills_day.rename(columns={'killmail_id': 'kill_count'}, inplace=True)
    return agg_kills_day


def agg_total_kills_by_day(df: pd.DataFrame) -> pd.DataFrame:
    agg_kills_day = df.groupby(['kill_time']).killmail_id.count().reset_index()
    agg_kills_day.rename(columns={'killmail_id': 'kill_count'}, inplace=True)
    return agg_kills_day


if __name__ == '__main__':
    pass
