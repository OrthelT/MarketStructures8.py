import re
from re import search

import pandas as pd
from numpy import unique
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import sessionmaker

from data_mapping import map_data, remap_reversable
from models import JoinedInvTypes

fitting_schema = [
    "type_id", "type_name", "group", "group_id", "category", "category_id",
    "fit", "fit_id", "doctrine", "doctrine_id",
    "ship", "ship_id", "qty"
]

fits_folder = 'fits'
moa = 'fits/[Moa,  WC-EN Shield DPS Moa v1.0].txt'

cargo_regex = r'x\d+'

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


def parse_fit(text_file) -> pd.DataFrame or None:
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

    unique_modules = unique(modules)

    for module in unique_modules:
        module_item = {'type_name': module, 'qty': modules.count(module), 'fit_name': name}
        module_list.append(module_item)

    fittings.extend(cargo_list)
    fittings.extend(module_list)

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


def update_Moa():
    fit_info = ['type_name', 'type_id', 'published', 'mass', 'capacity',
                'description', 'volume', 'packaged_volume', 'portion_size', 'radius',
                'graphic_id', 'icon_id', 'market_group_id', 'group_id']

    stmt = text("SELECT * FROM invTypes WHERE invTypes.typeID = 623")

    engine = create_engine(sde_db)
    with engine.connect() as conn:
        df = pd.read_sql_query(stmt, conn)

    type_cols = ['typeID', 'groupID', 'typeName', 'volume', 'portionSize', 'graphicID',
                 'iconID', 'marketGroupID']

    missing_col = ['type_id', 'group_id', 'type_name', 'packaged_volume', 'portion_size', 'graphic_id',
                   'icon_id', 'market_group_id', ]

    rename_cols = dict(zip(type_cols, missing_col))

    df.rename(columns=rename_cols, inplace=True)

    present = df.columns.intersection(missing_col)

    miss = [col for col in fit_info if col not in present]

    pres = ['type_id', 'group_id', 'type_name', 'packaged_volume', 'portion_size',
            'market_group_id', 'icon_id', 'graphic_id']

    add_cols = {'published': 1,
                'mass': 12000000,
                'capacity': 450,
                'description': "The Moa was designed as an all-out combat ship, and its heavy armament allows the Moa to tackle almost anything that floats in space. In contrast to its nemesis the Thorax, the Moa is most effective at long range where its railguns can rain death upon foes.",
                'volume': 101000,
                'radius': 202
                }

    for k, v in add_cols.items():
        df[k] = v
    df.drop(columns=['raceID', 'basePrice', 'soundID'], inplace=True)

    engine = create_engine(fittings_db)
    with engine.connect() as conn:
        df.to_sql('fittings_type', conn, if_exists='append', index=False)


def get_names_ORM(df) -> pd.DataFrame:
    df1 = map_data(df)

    names = df1['type_name'].unique().tolist()
    engine = create_engine(mkt_db)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        types = session.query(JoinedInvTypes.typeId, JoinedInvTypes.typeName, JoinedInvTypes.groupID,
                              JoinedInvTypes.groupName).filter(JoinedInvTypes.typeName.in_(names)).all()

        stmt = select(JoinedInvTypes.typeId).where(JoinedInvTypes.typeName.in_(names))
        result = session.execute(stmt)

    print(types)
    print(type(types))
    df2 = pd.DataFrame(types)

    df2, reversal = remap_reversable(df2)

    df3 = df.merge(df2, on='type_name', how='left')

    return df3

if __name__ == '__main__':
    pass
