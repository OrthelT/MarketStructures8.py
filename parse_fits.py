import re
from re import search
from dataclasses import dataclass, field
from typing import Optional, Generator, List
from collections import defaultdict


import pandas as pd
from numpy import unique
from sqlalchemy import create_engine, text, insert, delete, select
from sqlalchemy.orm import sessionmaker, Session

import logging_tool
from data_mapping import map_data, remap_reversable
from models import JoinedInvTypes, Fittings_FittingItem

fittings_fittingitem = Fittings_FittingItem

fitting_schema = [
    "type_id", "type_name", "group", "group_id", "category", "category_id",
    "fit", "fit_id", "doctrine", "doctrine_id",
    "ship", "ship_id", "qty"
]

fits_folder = 'fits'
CFI = 'fits/[Cyclone Fleet Issue,  2502 WC-EN C.txt'

cargo_regex = r'x\d+'
digit_end = r'\d$'

cargo_list = []

fittings_db = r"mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"
mkt_db = "sqlite:///market_orders.sqlite"
sde_db = r"sqlite:///C:/Users/User/PycharmProjects/ESI_Utilities/SDE/SDE sqlite-latest.sqlite"

from logging import getLogger

logger = logging_tool.configure_logging(log_name=__name__)

@dataclass
class FittingItem:
    flag: str
    quantity: int
    fit_id: int
    type_name: str
    ship_type_name: str
    fit_name: Optional[str] = None

    # Declare attributes that will be assigned in __post_init__
    type_id: int = field(init=False)
    type_fk_id: int = field(init=False)

    def __post_init__(self) -> None:
        self.type_id = self.get_type_id()
        self.type_fk_id = self.type_id  # optional alias
        self.details = self.get_fitting_details()
        self.description = self.details['description']

        #retrieve the fit name if we need it
        if self.fit_name is None and "name" in self.details:
            self.fit_name = self.details["name"]

    def get_type_id(self) -> int:
        engine = create_engine(fittings_db, echo=False)
        query = text("SELECT type_id FROM fittings_type WHERE type_name = :type_name")
        with engine.connect() as conn:
            result = conn.execute(query, {"type_name": self.type_name}).fetchone()
            return result[0] if result else -1  # return a sentinel or raise error

    def get_fitting_details(self) -> dict:
        engine = create_engine(fittings_db, echo=False)
        query = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
        with engine.connect() as conn:
            row = conn.execute(query, {"fit_id": self.fit_id}).fetchone()
            return dict(row._mapping) if row else {}


def parse_cargo(item) -> dict or None:
    t = search(cargo_regex, item)
    if t:
        entry = {'type_name': item[0:t.start() - 1], 'qty': item[t.start() + 1:t.end()]}
        return entry
    else:
        return None


def parse_fit(text_file, totals=False) -> pd.DataFrame or None:
    with open(text_file, 'r') as f:
        data = f.read()
    items = clean_items(data)
    cargo_list = []
    modules = []
    module_list = []
    # clean items
    fittings = []

    name = items[0]
    for i, item in enumerate(items[1:]):

        if re.search(cargo_regex, item) and i != 0:
            cargo_item = parse_cargo(item)
            cargo_item['fit_name'] = name
            cargo_list.append(cargo_item)

        else:
            modules.append(item)

    if totals:
        unique_modules = unique(modules)

        for module in unique_modules:
            module_item = {'type_name': module, 'qty': modules.count(module), 'fit_name': name}
            module_list.append(module_item)

    for module in modules:
        module_item = {'type_name': module, 'qty': 1, 'fit_name': name}
        module_list.append(module_item)

    fittings.extend(module_list)
    fittings.extend(cargo_list)

    df = pd.DataFrame(fittings, columns=['type_name', 'qty', 'fit_name'])

    df.type_name = df.type_name.apply(lambda x: x.strip())

    return df

def clean_items(items):
    clean_items = [x for x in items.splitlines()]
    clean_items = [x for x in clean_items if len(x) != 0]
    return clean_items

def get_names(df) -> pd.DataFrame:
    df1 = map_data(df)

    names = df1['type_name'].unique().tolist()
    engine = create_engine(mkt_db)
    data = []

    for name in names:
        fit_name = name

        with engine.connect() as conn:
            type_info = conn.execute(
                text("""SELECT * FROM joinedinvtypes 
                WHERE joinedinvtypes.typeName = :y"""),
                {"y": fit_name})
            data.append(type_info.fetchone())

    df2 = pd.DataFrame(data)

    return df2


def update_fittings_type(df, adds: dict) -> pd.DataFrame or None:
    df.drop(columns=['raceID', 'basePrice', 'soundID', 'portionSize'], inplace=True)

    fit_info = ['type_name', 'type_id', 'published', 'mass', 'capacity',
                'description', 'volume', 'packaged_volume', 'portion_size', 'radius',
                'graphic_id', 'icon_id', 'market_group_id', 'group_id']

    type_info = ['typeID', 'groupID', 'typeName', 'description', 'mass', 'volume',
                 'capacity', 'portionSize', 'raceID', 'basePrice', 'published',
                 'marketGroupID', 'iconID', 'soundID', 'graphicID']

    type_to_rename = ['typeName', 'typeID', 'groupID', 'volume',
                      'graphicID', 'iconID', 'marketGroupID']

    mew_name = ['type_name', 'type_id', 'group_id', 'packaged_volume', 'graphic_id',
                   'icon_id', 'market_group_id', ]

    rename_cols = dict(zip(type_to_rename, mew_name))

    df.rename(columns=rename_cols, inplace=True)

    add_cols = adds

    for k, v in add_cols.items():
        df[k] = v

    engine = create_engine(fittings_db)
    with engine.connect() as conn:
        df.to_sql('fittings_type', conn, if_exists='append', index=False)

    print("fit_updated")

def get_type_info_ORM(df, by_id: bool = False) -> pd.DataFrame:

    df1 = map_data(df)

    engine = create_engine(mkt_db)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        if by_id:
            ids = df1['type_id'].unique().tolist()
            id_types = session.query(JoinedInvTypes.typeId, JoinedInvTypes.typeName, JoinedInvTypes.groupID,
                                     JoinedInvTypes.groupName).filter(JoinedInvTypes.typeId.in_(ids)).all()
            join_value = 'type_id'
            df_const = id_types
            drop_list = [
                'type_name', 'group_id', 'group_name'
            ]
        else:
            names = df1['type_name'].unique().tolist()
            name_types = session.query(JoinedInvTypes.typeId, JoinedInvTypes.typeName, JoinedInvTypes.groupID,
                                       JoinedInvTypes.groupName).filter(JoinedInvTypes.typeName.in_(names)).all()
            join_value = 'type_name'
            df_const = name_types
            drop_list = [
                'type_id', 'group_id. group_name'
            ]

    df2 = pd.DataFrame(df_const)
    df.drop(columns=drop_list, inplace=True)
    df2, reversal = remap_reversable(df2)
    df3 = df.merge(df2, on=join_value, how='left')
    df3 = df3.reset_index(drop=True)

    return df3

def prepare_write_to_fitting_items(df: pd.DataFrame, fit_id: int) -> pd.DataFrame or None:
    if 'qty' in df.columns:
        df.rename(columns={'qty': 'quantity'}, inplace=True)

    df = df[['type_id', 'quantity']]
    type_id = df['type_id'].unique().tolist()

    engine = create_engine(fittings_db)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        flags = session.query(fittings_fittingitem.flag,
                              fittings_fittingitem.type_id,
                              fittings_fittingitem.type_fk_id).filter(
            fittings_fittingitem.type_id.in_(type_id))
    session.close()
    df2 = pd.DataFrame(flags)
    df2.drop_duplicates(subset=['type_id'], inplace=True)
    df2 = df2.reset_index(drop=True)

    df3 = pd.merge(df, df2, on='type_id', how='left')
    df3 = df3.reset_index(drop=True)

    df3['fit_id'] = fit_id
    df3['flag'] = df3.apply(lambda row: "HighSlot0" if pd.isnull(row['flag']) else row['flag'], axis=1)

    df3.type_fk_id = df3.type_fk_id.fillna(df3.type_id)



    df3.to_csv(f'data/{fit_id}_fittings_fittingitem.csv', index=False)

    return df3

def parse_quantities(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df[df['flag'].apply(lambda x: search(digit_end, x) is not None)]

    df2 = df2.reset_index(drop=True)
    df2 = df2[df2.quantity > 1]
    df2['flag'] = df2['flag'].str[:-1]
    qty = {}
    flg = {}

    for i, row in df2.iterrows():
        qty.update({row['type_id']: row['quantity']})
        flg.update({row['type_id']: row['flag']})

    drops = df[df.type_id.isin(qty.keys())]
    df.drop(index=drops.index, inplace=True)
    df = df.reset_index(drop=True)


    df3 = pd.DataFrame()
    for k, v in qty.items():
        for i in range(0, v):
            df3.loc[i, 'type_id'] = int(k)
            df3.loc[i, 'quantity'] = int(1)
            df3.loc[i, 'flag'] = str(flg[k]) + str(i + 1)
            df3.loc[i, 'fit_id'] = int(492)
            df3.loc[i, 'type_fk_id'] = int(k)
        df = pd.concat([df, df3]).reset_index(drop=True)

    df[['type_id', 'quantity', 'fit_id', 'type_fk_id']] = df[['type_id', 'quantity', 'fit_id', 'type_fk_id']].astype(
        int)

    engine = create_engine(fittings_db)
    with engine.connect() as conn:
        df.to_sql('fittings_fittingitem', conn, if_exists='append', index=False)

    return df

def parse_fit_number(fit_num: int) -> pd.DataFrame:
    engine = create_engine(fittings_db)
    with engine.connect() as conn:
        df = pd.read_sql_table('fittings_fittingitem', engine)
        df = df[df.fit_id == fit_num]
    engine.dispose()
    df3 = df.groupby('type_id')['quantity'].sum().reset_index()

    engine = create_engine(mkt_db)
    with engine.connect() as conn:
        df2 = pd.read_sql_table('JoinedInvTypes', conn)
    engine.dispose()

    df2 = df2[df2.typeID.isin(df3.type_id)]
    df2.reset_index(drop=True, inplace=True)
    df2.rename(columns={'typeName': 'type_name', 'typeID': 'type_id'}, inplace=True)
    df2 = df2[['type_id', 'type_name']].reset_index(drop=True)
    df4 = df3.merge(df2, on='type_id', how='left')
    df4 = df4.reset_index(drop=True)

    engine = create_engine(mkt_db)
    with engine.connect() as conn:
        df = pd.read_sql_table('Market_Stats', engine)
        engine.dispose()

    ids = df4['type_id'].unique().tolist()
    df5 = df[df.type_id.isin(ids)]
    df5.reset_index(drop=True, inplace=True)
    #

    df5 = df5[
        ['type_id', 'total_volume_remain', 'price_5th_percentile', 'days_remaining', 'avg_daily_volume', 'group_name',
         'category_name']]

    rename_cols = {'total_volume_remain': 'volume', 'price_5th_percentile': 'price', 'days_remaining': 'days',
                   'avg_daily_volume': 'avg_volume', 'group_name': 'group', 'category_name': 'category', }
    df6 = df4.merge(df5, on='type_id', how='left')
    df6.rename(columns=rename_cols, inplace=True)
    df6[['volume', 'price']] = df6[['volume', 'price']].round(0).reset_index(drop=True)
    df6['fits'] = (df6.volume / df6.quantity).round(0)
    return df

def slot_yielder() -> Generator[str, None, None]:
    """
    Yields EFT slot flags in correct order.
    Once primary sections are consumed, defaults to 'Cargo'.
    """
    corrected_order = ['LoSlot', 'MedSlot', 'HiSlot', 'RigSlot', 'DroneBay']
    for slot in corrected_order:
        yield slot
    while True:
        yield 'Cargo'

def process_fit(fit_file: str, fit_id: int) -> List[List]:
    fit = []
    qty = 1
    slot_gen = slot_yielder()
    current_slot = None
    ship_name = ""
    fit_name = ""
    slot_counters = defaultdict(int)

    with open(fit_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()

            if line.startswith("[") and line.endswith("]"):
                clean_name = line.strip('[]')
                parts = clean_name.split(',')
                ship_name = parts[0].strip()
                fit_name = parts[1].strip() if len(parts) > 1 else "Unnamed Fit"
                continue

            if line == "":
                # Only advance to the next slot when a blank line *after* content is found
                current_slot = next(slot_gen)
                continue

            if current_slot is None:
                # First block: assign the first slot only when we encounter the first item
                current_slot = next(slot_gen)

            # Parse quantity
            qty_match = re.search(r'\s+x(\d+)$', line)
            if qty_match:
                qty = int(qty_match.group(1))
                item = line[:qty_match.start()].strip()
            else:
                qty = 1
                item = line.strip()

            # Construct slot name
            if current_slot in {'LoSlot', 'MedSlot', 'HiSlot', 'RigSlot'}:
                suffix = slot_counters[current_slot]
                slot_counters[current_slot] += 1
                slot_name = f"{current_slot}{suffix}"
            else:
                slot_name = current_slot  # 'DroneBay' or 'Cargo'

            fitting_item = FittingItem(
                flag=slot_name,
                fit_id=fit_id,
                type_name=item,
                ship_type_name=ship_name,
                fit_name=fit_name,
                quantity=qty
            )

            fit.append([
                fitting_item.flag,
                fitting_item.quantity,
                fitting_item.type_id,
                fitting_item.fit_id,
                fitting_item.type_fk_id
            ])

    return fit



def insert_fitting_items(fit_data: List[List], session: Session):
    stmt = insert(Fittings_FittingItem).values([
        {
            "flag": row[0],
            "quantity": row[1],
            "type_id": row[2],
            "fit_id": row[3],
            "type_fk_id": row[4]
        }
        for row in fit_data
    ])
    session.execute(stmt)
    session.commit()

def replace_fit_items(fit_file: str, fit_id: int, session: Session):
    try:
        # Step 1: Parse
        fit_data = process_fit(fit_file, fit_id)
        if not fit_data:
            raise ValueError(f"No fitting items parsed from '{fit_file}'.")

        # Step 2: Validate
        items = []
        for i, row in enumerate(fit_data):
            flag, qty, type_id, fit_id_val, type_fk_id = row

            if not flag or not isinstance(flag, str):
                raise ValueError(f"Row {i}: Invalid flag: {flag}")
            if not isinstance(qty, int) or qty <= 0:
                raise ValueError(f"Row {i}: Invalid quantity: {qty}")
            if not isinstance(type_id, int) or type_id <= 0:
                raise ValueError(f"Row {i}: Invalid type_id: {type_id}")
            if fit_id_val != fit_id:
                raise ValueError(f"Row {i}: Mismatched fit_id: {fit_id_val}")
            if type_fk_id != type_id:
                raise ValueError(f"Row {i}: type_fk_id does not match type_id ({type_fk_id} != {type_id})")

            item = Fittings_FittingItem(
                flag=flag,
                quantity=qty,
                type_id=type_id,
                fit_id=fit_id,
                type_fk_id=type_fk_id
            )
            items.append(item)

        # Step 3: Show preview
        print("\nProposed replacement data:")
        for item in items:
            print("  ", item)

        confirm = input(f"\nReplace existing entries for fit_id={fit_id}? (y/n): ").strip().lower()
        if confirm != "y":
            print("Aborted by user. No changes made.")
            return

        # Step 4: Transaction
        session.execute(delete(Fittings_FittingItem).where(Fittings_FittingItem.fit_id == fit_id))
        session.add_all(items)
        session.commit()
        print(f"✅ Successfully replaced {len(items)} fitting items for fit_id={fit_id}.")

    except Exception as e:
        session.rollback()
        print(f"❌ Error occurred: {e}")

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    fit_file = "drake2501_39.txt"

    # Create engine
    engine = create_engine(fittings_db, echo=False)

    # Create configured session factory
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        replace_fit_items(fit_file, fit_id=39, session=session)





