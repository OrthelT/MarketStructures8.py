import pandas as pd
from google_sheet_updater import gsheet_image_updater

icon_id = 23913
model = f'"https://images.evetech.net/types/{icon_id}/render?size=64"'
print(model)


def construct_URL(df: pd.DataFrame):
    df = df.copy()
    df.loc[:, "URLs"] = df["type_id"].apply(lambda x: f'https://images.evetech.net/types/{x}/render?size=64')

    # df["URLs"] = df["type_id"].apply(lambda x: f'https://images.evetech.net/types/{x}/render?size=64')
    return df


if __name__ == "__main__":
    pass


# mess = gsheet_image_updater(df2)
# print(mess)
