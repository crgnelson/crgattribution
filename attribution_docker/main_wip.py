import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from marketing_attribution_models import MAM
from collections import defaultdict
import matplotlib.pyplot as plt
from markovclick.models import MarkovClickstream
from markovclick.viz import visualise_markov_chain
import os
import graphviz
import matplotlib as mpl
from pandas.io import gbq
import pandas_gbq
import glob
from pylab import *
import tempfile
import json
from datetime import timedelta
import seaborn as sns
import gc
from datetime import datetime
import re

# Parameters from environment
conversion_type = os.environ.get("CONVERSION_TYPE", "Trial")  # Trial or Subscription
lookback_window = int(os.environ.get("LOOKBACK_WINDOW", 90))  # 30, 60, 90

project = "ft-customer-analytics"
client = bigquery.Client(project=project, location="EU")

# Constants
ids = "user_guid"
date = "attribution_visit_start_time"
touchpoint = "touchpoint"
transaction = "converting_visit"

# Dates
end_date = pd.Timestamp.today().date() - pd.DateOffset(days=1)
start_date = end_date - pd.DateOffset(days=14)
start_date = start_date.date()
end_date = end_date.date()

ltv_table_id = "ft-customer-analytics.crg_nniu.ltv_last_15_days"

def table_exists(client, table_id):
    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False

def calculate_removal_effect(row):
    attr = row.get("attribution_markov_algorithmic")
    ltv = row.get("ltv_acquisition_capped_12m")
    channels = row.get("channels_agg")

    if pd.isna(attr) or pd.isna(channels):
        return None

    attr_parts = attr.split(">")
    channel_parts = channels.split(">")

    if len(attr_parts) != len(channel_parts):
        return None

    try:
        new_parts = [
            f"{c.strip()}: {float(a.strip()) * ltv}"
            for c, a in zip(channel_parts, attr_parts)
        ]
        return " > ".join(new_parts)
    except:
        return None

def sanitize_column_name(col_name):
    # Remove patterns like '_0.3', '0.6', etc.
    sanitized = re.sub(r"(_)?\d+\.\d+", "", col_name)
    # Replace multiple underscores with a single underscore
    sanitized = re.sub(r"_+", "_", sanitized)
    # Remove leading or trailing underscores
    sanitized = sanitized.strip("_")
    return sanitized

def run_pipeline(conversion_type, lookback_window):
    table_id = f"ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_{lookback_window}_days_lookback_table"
    user_df_all = pd.DataFrame()
    attribution_df_all = pd.DataFrame()
    markov_matrix_all = pd.DataFrame()
    removal_effects_all = pd.DataFrame()

    print(f"Running: {conversion_type} | {lookback_window}-day window")

    for current_date in pd.date_range(start_date, end_date):
        print(f"\n⏳ {current_date.date()}")

        query = f"""
        SELECT * FROM {table_id}
        WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
        """
        df = client.query(query).to_dataframe()

        if df.empty:
            print("No data.")
            continue

        df["original_transaction"] = df["converting_visit"]
        df = df[df["conversion_type"] == conversion_type].drop(columns=["conversion_type"])
        df["user_max_date"] = df.groupby(ids)[date].transform("max")
        df[transaction] = 0
        df.loc[(df[date] == df["user_max_date"]) & (df["original_transaction"] == 1), transaction] = 1
        df.drop(columns=["user_max_date"], inplace=True)
        df.sort_values([ids, date], inplace=True)
        df["run_date"] = current_date.date()

        try:
            attributions = MAM(
                df,
                group_channels=True,
                channels_colname=touchpoint,
                journey_with_conv_colname=transaction,
                group_channels_by_id_list=[ids],
                group_timestamp_colname=date,
                create_journey_id_based_on_conversion=True,
            )

            attributions.attribution_last_click()
            attributions.attribution_first_click()
            attributions.attribution_position_based([0.3, 0.3, 0.4])
            attributions.attribution_time_decay(0.6, frequency=7)
            attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

            user_df = attributions.as_pd_dataframe()
            user_df.columns = [sanitize_column_name(c) for c in user_df.columns]
            user_df["num_touchpoints"] = user_df["channels_agg"].str.split(" > ").apply(len)
            user_df["run_date"] = current_date.date()
            user_df["conversion_type"] = conversion_type
            user_df["conversion_window"] = lookback_window
            user_df["user_guid"] = user_df["journey_id"].str.extract(r'id:(.*)_J:\d+')[0]

            df["conversion_visit_timestamp_date"] = pd.to_datetime(df["conversion_visit_timestamp"]).dt.date
            product_arrangement_df = df[["user_guid", "conversion_visit_timestamp_date", "product_arrangement_id", "is_app_conversion"]].drop_duplicates()
            user_df = user_df.merge(
                product_arrangement_df,
                left_on=["user_guid", "run_date"],
                right_on=["user_guid", "conversion_visit_timestamp_date"],
                how="left"
            ).drop(columns=["conversion_visit_timestamp_date"])

            user_df_all = pd.concat([user_df_all, user_df], ignore_index=True)

            attribution_df = attributions.group_by_channels_models
            attribution_df.columns = [sanitize_column_name(c) for c in attribution_df.columns]
            attribution_df["run_date"] = current_date.date()
            attribution_df["conversion_type"] = conversion_type
            attribution_df["conversion_window"] = lookback_window
            attribution_df_all = pd.concat([attribution_df_all, attribution_df], ignore_index=True)

            markov_matrix = attribution_markov[2].round(3).rename(
                index=lambda x: x.replace("(inicio)", "(start)"),
                columns=lambda x: x.replace("(inicio)", "(start)")
            ).reset_index()
            markov_matrix = pd.melt(markov_matrix, id_vars="index", var_name="destination", value_name="probability")
            markov_matrix.columns = ["source", "destination", "probability"]
            markov_matrix["run_date"] = current_date.date()
            markov_matrix["conversion_type"] = conversion_type
            markov_matrix["conversion_window"] = lookback_window
            markov_matrix_all = pd.concat([markov_matrix_all, markov_matrix], ignore_index=True)

            removal_df = attribution_markov[3].round(3)[["removal_effect"]]
            removal_df = (removal_df / removal_df.sum()) * 100
            removal_df.reset_index(inplace=True)
            removal_df.columns = ["channel", "removal_effect"]
            removal_df["run_date"] = current_date.date()
            removal_df["conversion_type"] = conversion_type
            removal_df["conversion_window"] = lookback_window
            removal_effects_all = pd.concat([removal_effects_all, removal_df], ignore_index=True)

            print("✅ Success")
        except Exception as e:
            print(f"❌ Failed: {e}")

    print("\n📊 Merging with LTV...")
    ltv_df = client.query(f"SELECT * FROM {ltv_table_id}").to_dataframe()
    ltv_df = ltv_df.dropna(subset=["ltv_acquisition_capped_12m"])
    ltv_df["ltv_acquisition_capped_12m"] = ltv_df["ltv_acquisition_capped_12m"].astype(float)
    ltv_df["product_order_timestamp"] = pd.to_datetime(ltv_df["product_order_timestamp"], utc=True).dt.date

    user_df_all = user_df_all[user_df_all["conversion_value"] == 1].copy()
    user_df_all["product_arrangement_id"] = user_df_all["product_arrangement_id"].fillna(0)
    user_df_all["run_date"] = pd.to_datetime(user_df_all["run_date"]).dt.date

    user_df_all = pd.merge(
        user_df_all,
        ltv_df,
        left_on=["product_arrangement_id", "run_date"],
        right_on=["product_arrangement_id", "product_order_timestamp"],
        how="left"
    ).drop(columns=["product_order_timestamp"])

    user_df_all["removal_effect_ltv"] = user_df_all.apply(calculate_removal_effect, axis=1)

    user_df_all = user_df_all.drop_duplicates()

    print(f"\n📤 Uploading all outputs to BigQuery...")
    # Define job config
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date"
        )
    )

    dataframes = {
        "ft-customer-analytics.crg_nniu_attribution.test_attribution_user_df_all": user_df_all,
        "ft-customer-analytics.crg_nniu_attribution.test_attribution_channel_level": attribution_df_all,
        "ft-customer-analytics.crg_nniu_attribution.test_attribution_markov_matrix": markov_matrix_all,
        "ft-customer-analytics.crg_nniu_attribution.test_attribution_removal_effects": removal_effects_all,
    }

    for destination_table, df in dataframes.items():
        if df.empty:
            print(f"⚠️ Skipping {destination_table} — empty DataFrame.")
            continue

        table_is_ready = table_exists(client, destination_table)

        if table_is_ready:
            run_dates = pd.to_datetime(df["run_date"].dropna().unique()).astype(str)
            for run_date in run_dates:
                delete_query = f"""
                DELETE FROM `{destination_table}`
                WHERE run_date = DATE('{run_date}')
                AND conversion_type = '{conversion_type}'
                AND conversion_window = {lookback_window}
                """
                try:
                    client.query(delete_query).result()
                    print(f"🧹 Cleared data for {run_date} in {destination_table}")
                except Exception as e:
                    print(f"⚠️ Could not delete partition {run_date} from {destination_table}: {e}")
        else:
            print(f"📁 Table not found — skipping delete for {destination_table}. It will be created on upload.")

        try:
            client.load_table_from_dataframe(df.reset_index(drop=True), destination_table, job_config=job_config).result()
            print(f"✅ Uploaded data to {destination_table}")
        except Exception as e:
            print(f"❌ Upload failed for {destination_table}: {e}")


# if __name__ == "__main__":
#     if conversion_type not in ["Trial", "Subscription"] or lookback_window not in [30, 60, 90]:
#         raise ValueError(f"Invalid configuration: {conversion_type} / {lookback_window}")
#     run_pipeline(conversion_type, lookback_window)

if __name__ == "__main__":
    conversion_types = ["Trial", "Subscription"]
    lookback_windows = [30, 60, 90]

    for conv_type in conversion_types:
        for window in lookback_windows:
            print(f"\n🚀 Running: {conv_type} | {window}-day window")
            try:
                run_pipeline(conv_type, window)
            except Exception as e:
                print(f"❌ Failed: {conv_type} | {window}d | Error: {e}")
