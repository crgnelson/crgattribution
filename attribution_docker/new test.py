################################################ Where script begins ################################################
from marketing_attribution_models import MAM
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import gc
from datetime import datetime, timedelta
from google.cloud import bigquery

################################################# Data Loading  #########################################

project = "ft-customer-analytics"
location = "EU"
client = bigquery.Client(project=project, location=location)

table_id = "ft-customer-analytics.crg_nniu.historical_conversion_visit_static"

# If you have specific dates you want to analyze, define them here
# For example, to process data from '2024-11-01' to '2024-11-30'
# overall_start_date = datetime(2024, 11, 1).date()
# overall_end_date = datetime(2024, 11, 30).date()

# Alternatively, get the date range from the dataset
# Fetch the minimum and maximum dates from the data
date_range_query = f"""
SELECT
    MIN(DATE(conversion_visit_timestamp)) AS min_date,
    MAX(DATE(conversion_visit_timestamp)) AS max_date
FROM {table_id}
"""

date_range_job = client.query(date_range_query)
date_range_result = date_range_job.to_dataframe()
overall_start_date = date_range_result['min_date'][0]
overall_end_date = date_range_result['max_date'][0]

# Fetch the list of unique dates in conversion_visit_timestamp within the date range
dates_query = f"""
SELECT DISTINCT DATE(conversion_visit_timestamp) as conversion_date
FROM {table_id}
WHERE DATE(conversion_visit_timestamp) BETWEEN '{overall_start_date.strftime('%Y-%m-%d')}' AND '{overall_end_date.strftime('%Y-%m-%d')}'
ORDER BY conversion_date
"""

dates_job = client.query(dates_query)
dates_df = dates_job.to_dataframe()
unique_dates = dates_df['conversion_date'].tolist()

# Initialize aggregated DataFrames
attribution_df_all_subs_90 = pd.DataFrame()
normalized_removal_effects_all_subs_90 = pd.DataFrame()
markov_transition_matrix_all_subs_90 = pd.DataFrame()
user_df_all_subs_90 = pd.DataFrame()

# Loop over each unique date in conversion_visit_timestamp
for conversion_date in unique_dates:
    # Set end_date to the conversion_date being processed
    end_date = conversion_date

    # Set start_date to 90 days before the end_date
    start_date = end_date - timedelta(days=90)

    # Build the query to fetch data up to the current conversion_date
    query = f"""
    SELECT *
    FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    """
    print(f"Fetching data for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} (Conversion Date: {end_date.strftime('%Y-%m-%d')})")

    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe()

    # Check if df is empty
    if df.empty:
        print(f"No data for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        continue

    # Define column names
    ids = "user_guid"
    date = "attribution_visit_start_time"
    touchpoint = "touchpoint"
    transaction = "converting_visit"

    # Filter data for Subscription conversion type
    sub_df = df[df["conversion_type"] == "Subscription"].drop(columns=["conversion_type"])

    # Delete main df to release memory
    del df
    gc.collect()

    # Adjust end_date and start_date for filtering in the DataFrame
    end_date_tz = pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
    start_date_tz = pd.to_datetime(start_date).tz_localize("UTC").normalize()

    try:
        # Filter the DataFrame for the 90-day period up to the current conversion_date
        sub_df = sub_df[
            (sub_df[date] >= start_date_tz) & (sub_df[date] <= end_date_tz)
        ]
        if sub_df.empty:
            print(f"No data for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            continue

        # Set the conversion flag based on the conversion_date
        sub_df[transaction] = 0
        sub_df.loc[sub_df["conversion_visit_timestamp"].dt.date == end_date, transaction] = 1

        # Sort the DataFrame
        sub_df = sub_df.sort_values([ids, date], ascending=[False, True])

        # Initialize the MAM class
        attributions = MAM(
            sub_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        # Apply various attribution models
        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7  # Frequency is in hours
        )
        attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

        # Export user-level attribution data
        user_df_temp = attributions.as_pd_dataframe()

        # Calculate the number of touchpoints for each journey
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )

        # Add a "run_date" column (conversion_date)
        user_df_temp["run_date"] = end_date

        # Append the result to the aggregated DataFrame with _90 suffix
        user_df_all_subs_90 = pd.concat(
            [user_df_all_subs_90, user_df_temp], ignore_index=True
        )

        ##################################

        # Process Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )

        # Reset index to convert the index (pages) to a column
        markov_transition_matrix.reset_index(inplace=True)

        # Melt the DataFrame to convert columns to rows
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )

        # Rename columns
        markov_transition_matrix.columns = ["source", "destination", "probability"]

        # Add a "run_date" column (conversion_date)
        markov_transition_matrix["run_date"] = end_date

        # Append the result to the aggregated DataFrame with _90 suffix
        markov_transition_matrix_all_subs_90 = pd.concat(
            [markov_transition_matrix_all_subs_90, markov_transition_matrix],
            ignore_index=True,
        )

        # Process removal effects
        removal_effect_matrix = attribution_markov[3].round(3)

        # Extract the removal effect column as a DataFrame
        removal_effect_values = removal_effect_matrix[["removal_effect"]]

        # Normalize the values
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

        # Create a new DataFrame with the normalized values and 'channel' index
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=removal_effect_values.index, columns=["removal_effect"]
        )

        # Add a "run_date" column (conversion_date)
        normalized_removal_effects["run_date"] = end_date

        # Add the original removal effects (before normalization) as a new column
        normalized_removal_effects[
            "removal_effect_raw"
        ] = removal_effect_values.values.flatten()

        # Reset the index to make 'channel' a regular column before saving
        normalized_removal_effects.reset_index(inplace=True)

        # Rename the default 'index' column to 'channel'
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)

        # Append the result to the aggregated DataFrame with _90 suffix
        normalized_removal_effects_all_subs_90 = pd.concat(
            [normalized_removal_effects_all_subs_90, normalized_removal_effects],
            ignore_index=True,
        )

        # Process attribution by channels and models
        attribution_df = attributions.group_by_channels_models

        # Add a "run_date" column (conversion_date)
        attribution_df["run_date"] = end_date

        # Ensure column names adhere to BigQuery's naming rules
        attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
        attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

        # Append the result to the aggregated DataFrame with _90 suffix
        attribution_df_all_subs_90 = pd.concat(
            [attribution_df_all_subs_90, attribution_df], ignore_index=True
        )

    except Exception as e:
        print(
            f"An error occurred for the date {end_date.strftime('%Y-%m-%d')}: {e}"
        )

# After the loop, add the conversion window column
attribution_df_all_subs_90["conversion_window"] = 90
normalized_removal_effects_all_subs_90["conversion_window"] = 90
markov_transition_matrix_all_subs_90["conversion_window"] = 90
user_df_all_subs_90["conversion_window"] = 90

attribution_df_all_subs_90["conversion_window"] = 90
normalized_removal_effects_all_subs_90["conversion_window"] = 90
markov_transition_matrix_all_subs_90["conversion_window"] = 90
user_df_all_subs_90["conversion_window"] = 90

attribution_df_all = attribution_df_all_subs_90
normalized_removal_effects_all = normalized_removal_effects_all_subs_90
markov_transition_matrix_all = markov_transition_matrix_all_subs_90
user_df_all = user_df_all_subs_90

#Configure the load job
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE. # WRITE_APPEND
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="run_date"
    ),
)

dataframes = {
    "ft-customer-analytics.crg_nniu.test_attribution_markov_transition_matrix_all": markov_transition_matrix_all,
    "ft-customer-analytics.crg_nniu.test_attribution_normalized_removal_effects_all": normalized_removal_effects_all,
    "ft-customer-analytics.crg_nniu.test_attribution_user_df_all": user_df_all,
    "ft-customer-analytics.crg_nniu.test_attribution_df_all": attribution_df_all,
}

for destination_table, dataframe in dataframes.items():
    try:
        load_job = client.load_table_from_dataframe(
            dataframe.reset_index(drop=True), destination_table, job_config=job_config
        )
        load_job.result()
        print(f"Load job for {destination_table} completed successfully.")
    except Exception as e:
        print(f"Error loading data to {destination_table}: {e}")