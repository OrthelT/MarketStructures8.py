import pandas as pd
from google_sheet_updater import gsheet_image_updater

icon_id = 23913
model = f'"https://images.evetech.net/types/{icon_id}/render?size=64"'
print(model)

ships = 'output/ships.csv'

df = pd.read_csv('data/shipids.csv')
print(df)
df["URLs"] = df["type_id"].apply(lambda x: f'https://images.evetech.net/types/{x}/render?size=64')
print(df.head())
gsheet_image_updater(df)

# mess = gsheet_image_updater(df2)
# print(mess)
