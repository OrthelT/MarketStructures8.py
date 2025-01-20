import re
from re import search

import pandas as pd
from numpy import unique
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from data_mapping import map_data, remap_reversable
from models import JoinedInvTypes, Fittings_FittingItem as fittings_fittingitem

fitting_schema = [
    "type_id", "type_name", "group", "group_id", "category", "category_id",
    "fit", "fit_id", "doctrine", "doctrine_id",
    "ship", "ship_id", "qty"
]

fits_folder = 'fits'
moa = 'fits/[Moa,  WC-EN Shield DPS Moa v1.0].txt'

cargo_regex = r'x\d+'
digit_end = r'\d$'

cargo_list = []

fittings_db = r"mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"
mkt_db = "sqlite:///market_orders.sqlite"
sde_db = r"sqlite:///C:/Users/User/PycharmProjects/ESI_Utilities/SDE/SDE sqlite-latest.sqlite"


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

    return df4


def get_type_info_ORM(df) -> pd.DataFrame:
    df1 = map_data(df)

    names = df1['type_name'].unique().tolist()
    engine = create_engine(mkt_db)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        types = session.query(JoinedInvTypes.typeId, JoinedInvTypes.typeName, JoinedInvTypes.groupID,
                              JoinedInvTypes.groupName).filter(JoinedInvTypes.typeName.in_(names)).all()

    df2 = pd.DataFrame(types)

    df2, reversal = remap_reversable(df2)

    df3 = df.merge(df2, on='type_name', how='left')
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


if __name__ == '__main__':
    pass