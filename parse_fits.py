import re
from re import search

import pandas as pd
from numpy import unique
from sqlalchemy import create_engine, text

from data_mapping import map_data

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

    #
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
            res = conn.execute(text("SELECT * FROM joinedinvtypes WHERE joinedinvtypes.typeName = :y"), {"y": fit_name})
            data.append(res.fetchone())

    df2 = pd.DataFrame(data)

    return df2


if __name__ == '__main__':
    pass
