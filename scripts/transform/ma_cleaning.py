import pandas as pd

def clean_dataframe(df: pd.DataFrame):
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    if "tanggal_register" in df.columns:
        df["tanggal_register"] = pd.to_datetime(
            df["tanggal_register"], errors="coerce"
        )

    df["load_timestamp"] = pd.Timestamp.now()

    return df