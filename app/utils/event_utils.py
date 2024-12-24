import pandas as pd

def change_names_columns(df):
    renamed_df = df.rename(
        columns={
            "City": "city",
            "Country": "country_txt",
            "Perpetrator": "gname",
            "Weapon": "weaptype1_txt",
            "Injuries": "nwound",
            "Fatalities": "nkill",
            "Description": "summary",
        }
    )
    renamed_df["Date"] = pd.to_datetime(renamed_df["Date"], format="%d-%b-%y", errors="coerce")
    renamed_df["iyear"] = renamed_df["Date"].dt.year
    renamed_df["imonth"] = renamed_df["Date"].dt.month
    renamed_df["iday"] = renamed_df["Date"].dt.day
    return renamed_df.drop(columns=["Date"])

def fill_missing_columns(reference_df, target_df):
    missing_cols = set(reference_df.columns) - set(target_df.columns)
    for col in missing_cols:
        target_df[col] = None
    return target_df

def reorder_columns(reference_df, target_df):
    return target_df[reference_df.columns]

def merge_dataframes(df1, df2):
    return pd.concat([df1, df2], ignore_index=True).replace({pd.NA: None})

def extract_columns(df):
    relevant_columns = [
        "iyear", "imonth", "iday", "region_txt", "country_txt", "city",
        "latitude", "longitude", "attacktype1_txt", "attacktype2_txt", "attacktype3_txt",
        "nkill", "nwound", "gname", "gname2", "gname3", "targtype1_txt", "targtype2_txt",
        "targtype3_txt", "summary"
    ]
    return df[relevant_columns]

def convert_to_mongo_schema(row):
    return {
        "date": {
            "year": row.get("iyear"),
            "month": row.get("imonth"),
            "day": row.get("iday"),
        },
        "location": {
            "region": row.get("region_txt", ""),
            "country": row.get("country_txt", ""),
            "city": row.get("city", ""),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
        },
        "groups_involved": [
            group for group in [row.get("gname"), row.get("gname2"), row.get("gname3")]
            if group and group.lower() != "unknown"
        ],
        "attack_type": [
            attack for attack in [
                row.get("attacktype1_txt"),
                row.get("attacktype2_txt"),
                row.get("attacktype3_txt"),
            ]
            if attack and attack.lower() != "unknown"
        ],
        "target_type": [
            target for target in [
                row.get("targtype1_txt"),
                row.get("targtype2_txt"),
                row.get("targtype3_txt"),
            ]
            if target and target.lower() != "unknown"
        ],
        "casualties": {
            "fatalities": int(row["nkill"]) if pd.notnull(row["nkill"]) else 0,
            "injuries": int(row["nwound"]) if pd.notnull(row["nwound"]) else 0,
            "score": (row.get("nkill", 0) or 0) * 2 + (row.get("nwound", 0) or 0),
        },
        "summary": row.get("summary", ""),
    }
