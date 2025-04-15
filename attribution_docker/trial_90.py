from marketing_attribution_models import MAM
import pandas as pd
import numpy as np
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
from google.cloud import bigquery

################################################# Data Loading  #########################################

project = "ft-customer-analytics"
location = "EU"
client = bigquery.Client(project=project, location=location)

table_id = "ft-customer-analytics.crg_nniu.conversion_visit_static_90"

# Define the date range for processing
start_date = pd.to_datetime("2023-01-01")  # Start of the range
end_date = pd.to_datetime("2024-12-07")  # End of the range

################################################# Output DataFrames  #########################################

attribution_df_all_trial_90 = pd.DataFrame()
normalized_removal_effects_all_trial_90 = pd.DataFrame()
markov_transition_matrix_all_trial_90 = pd.DataFrame()
user_df_all_trial_90 = pd.DataFrame()

################################################# Process Data for Each Day #########################################

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")

    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe()

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue

    ################################################# Data Cleaning  #########################################

    ids = "user_guid"
    date = "attribution_visit_start_time"
    touchpoint = "touchpoint"
    transaction = "converting_visit"

    trial_df = df[df["conversion_type"] == "Trial"].drop(columns=["conversion_type"])

    trial_df["user_max_date"] = trial_df.groupby(ids)[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[trial_df[date] == trial_df["user_max_date"], transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

    trial_df["run_date"] = current_date.date()

    ################################################# MAM Initialization #########################################

    try:
        # Initialize the MAM class
        attributions = MAM(
            trial_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date
        product_arrangement_df = df[['user_guid', 'conversion_visit_timestamp_date', 'product_arrangement_id']].drop_duplicates()

        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )

        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)

        # Now concatenate user_df_temp into user_df_all_trial_90
        user_df_all_trial_90 = pd.concat(
            [user_df_all_trial_90, user_df_temp], ignore_index=True
        )

        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_trial_90 = pd.concat(
            [markov_transition_matrix_all_trial_90, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_trial_90 = pd.concat(
            [normalized_removal_effects_all_trial_90, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_trial_90 = pd.concat(
            [attribution_df_all_trial_90, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_trial_90["conversion_window"] = 90
normalized_removal_effects_all_trial_90["conversion_window"] = 90
markov_transition_matrix_all_trial_90["conversion_window"] = 90
user_df_all_trial_90["conversion_window"] = 90

attribution_df_all_trial_90["conversion_type"] = "Trial"
normalized_removal_effects_all_trial_90["conversion_type"] = "Trial"
markov_transition_matrix_all_trial_90["conversion_type"] = "Trial"
user_df_all_trial_90["conversion_type"] = "Trial"

# Combine all data into final DataFrames
attribution_df_all = attribution_df_all_trial_90
normalized_removal_effects_all = normalized_removal_effects_all_trial_90
markov_transition_matrix_all = markov_transition_matrix_all_trial_90
user_df_all = user_df_all_trial_90

def sanitize_column_name(col_name):
    # Remove patterns like '_0.3', '0.6', etc.
    sanitized = re.sub(r"(_)?\d+\.\d+", "", col_name)
    # Replace multiple underscores with a single underscore
    sanitized = re.sub(r"_+", "_", sanitized)
    # Remove leading or trailing underscores
    sanitized = sanitized.strip("_")
    return sanitized

# Create a mapping from original to sanitized column names
renamed_columns = {col: sanitize_column_name(col) for col in user_df_all.columns}

# Rename the DataFrame columns
user_df_all = user_df_all.rename(columns=renamed_columns)

########################################### Merge with LTV #########################################

client = bigquery.Client(project='ft-customer-analytics')
ltv_table_id = 'ft-customer-analytics.crg_nniu.ltv_static'

query = f"""
    SELECT *
    FROM
        {ltv_table_id}
"""

query_job = client.query(query)
ltv_df = query_job.to_dataframe()

group_columns = [col for col in ltv_df.columns if col != 'ltv_acquisition_capped_12m']

# Group by all columns except 'ltv_acquisition_capped_12m' and calculate its mean
ltv_df = ltv_df.groupby(group_columns, as_index=False).agg(
    ltv_acquisition_capped_12m=('ltv_acquisition_capped_12m', 'mean')
)

# Extract user_guid from journey_id
user_df_all['user_guid'] = user_df_all['journey_id'].str.extract(r'id:(.*)_J:0')[0]

# Convert date columns
ltv_df['product_order_timestamp'] = pd.to_datetime(ltv_df['product_order_timestamp'], utc=True)
user_df_all['run_date'] = pd.to_datetime(user_df_all['run_date'], utc=True)

# Convert date columns to date type
ltv_df['product_order_timestamp'] = ltv_df['product_order_timestamp'].dt.date
user_df_all['run_date'] = user_df_all['run_date'].dt.date

# Convert ltv_acquisition_capped_12m to float
ltv_df['ltv_acquisition_capped_12m'] = ltv_df['ltv_acquisition_capped_12m'].astype(float)

# Filter for conversion_value == 1
user_df_all = user_df_all[user_df_all["conversion_value"] == 1]

# Handle missing values in 'product_arrangement_id'
user_df_all['product_arrangement_id'] = user_df_all['product_arrangement_id'].fillna(0)

# Merge user_df_all with ltv_df
user_df_all = pd.merge(
    user_df_all,
    ltv_df,
    left_on=['product_arrangement_id', 'run_date'],
    right_on=['product_arrangement_id', 'product_order_timestamp'],
    how='left'
)

user_df_all = user_df_all.drop(columns=['user_guid_y'])
user_df_all = user_df_all.rename(columns={'user_guid_x': 'user_guid'})

def calculate_removal_effect(row):
    attr = row['attribution_markov_algorithmic']
    ltv = row['ltv_acquisition_capped_12m']
    channels = row['channels_agg']

    if pd.isna(attr) or pd.isna(channels):
        return np.nan

    attr_parts = attr.split('>')
    channel_parts = channels.split('>')

    if len(attr_parts) != len(channel_parts):
        return np.nan

    new_parts = []
    for channel, part in zip(channel_parts, attr_parts):
        channel = channel.strip()
        part = part.strip()
        try:
            val = float(part)
            multiplied_val = val * ltv
            formatted_val = f"{multiplied_val}"
            new_parts.append(f"{channel}: {formatted_val}")
        except ValueError:
            return np.nan

    return ' > '.join(new_parts)

def process_user_df(user_df):
    # Apply the function to create the 'removal_effect_ltv' column
    user_df['removal_effect_ltv'] = user_df.apply(calculate_removal_effect, axis=1)
    user_df = user_df.dropna(subset=['removal_effect_ltv']).copy()

    # Split 'removal_effect_ltv' into a list of 'channel: ltv' strings
    user_df['channel_ltv_list'] = user_df['removal_effect_ltv'].str.split(' > ')

    # Explode the list to have one 'channel: ltv' per row
    df_exploded = user_df.explode('channel_ltv_list')

    # Split each 'channel_ltv' into 'channel' and 'ltv'
    df_exploded[['channel', 'ltv']] = df_exploded['channel_ltv_list'].str.split(': ', n=1, expand=True)

    # Convert 'ltv' to numeric, handling any non-numeric values gracefully
    df_exploded['ltv'] = pd.to_numeric(df_exploded['ltv'], errors='coerce')

    # Group by 'channel' and 'run_date', then calculate the mean LTV
    average_ltv_per_channel = (
        df_exploded.groupby(['channel', 'run_date'])['ltv']
        .mean()
        .reset_index()
    )

    # Rename columns for clarity
    average_ltv_per_channel.rename(columns={'ltv': 'average_ltv'}, inplace=True)

    return average_ltv_per_channel

# Applying the process to the DataFrame and storing the results
user_df_trial_avg = process_user_df(user_df_all)

# Adding the 'conversion_window' and 'conversion_type' columns to the DataFrame
user_df_trial_avg['conversion_window'] = 90
user_df_trial_avg['conversion_type'] = 'Trial'

# Assigning the result to average_ltv_per_channel
average_ltv_per_channel = user_df_trial_avg

normalized_removal_effects_all = normalized_removal_effects_all[
    normalized_removal_effects_all['removal_effect'] != 0
]

# Merge normalized_removal_effects_all with average_ltv_per_channel
normalized_removal_effects_all = pd.merge(
    normalized_removal_effects_all,
    average_ltv_per_channel,
    on=["channel", "conversion_window", "conversion_type", "run_date"],
    how="left"
)

# Configure the load job
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Options: WRITE_TRUNCATE or WRITE_APPEND
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="run_date"
    ),
)

# Prepare dataframes for loading to BigQuery with updated table names
dataframes = {
    "ft-customer-analytics.crg_nniu.historical_markov_transition_matrix_all": markov_transition_matrix_all,
    "ft-customer-analytics.crg_nniu.historical_normalized_removal_effects_all": normalized_removal_effects_all,
    "ft-customer-analytics.crg_nniu.historical_user_df_all": user_df_all,
    "ft-customer-analytics.crg_nniu.historical_attribution_df_all": attribution_df_all
}

# Load dataframes to BigQuery
for destination_table, dataframe in dataframes.items():
    try:
        load_job = client.load_table_from_dataframe(
            dataframe.reset_index(drop=True), destination_table, job_config=job_config
        )
        load_job.result()
        print(f"Load job for {destination_table} completed successfully.")
    except Exception as e:
        print(f"Error loading data to {destination_table}: {e}")
