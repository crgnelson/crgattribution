################################################ Where sciprt begins ################################################
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
#data_table.enable_dataframe_formatter()
#auth.authenticate_user()

client = bigquery.Client(project="ft-customer-analytics")


table_id = "ft-customer-analytics.crg_nniu.temp_jan"


################################################# Data loading and cleaning  #########################################

query = f"""
SELECT * FROM {table_id}
"""

query_job = client.query(query)

df = query_job.to_dataframe()

ids = "user_guid"
date = "attribution_visit_start_time"
touchpoint = "touchpoint"
transaction = "converting_visit"

trial_df = df[df["conversion_type"] == "Trial"]
sub_df = df[df["conversion_type"] == "Subscription"]
# registration_df = df[df["conversion_type"] == "Registration"]

trial_df = trial_df.drop(columns=["conversion_type"])
sub_df = sub_df.drop(columns=["conversion_type"])
# registration_df = registration_df.drop(columns=["conversion_type"])

# Trim df as subset with country, user status, and product type, which will be used to join with attribution outputs
df_filtered = df[df["converting_visit"] == 1]
df_filtered = df[
    [
        "user_guid",
        "conversion_visit_timestamp",
        "b2c_product_type",
        "product_arrangement_id",
        "b2c_product_name_and_term",
    ]
]
latest_timestamp_per_user = df_filtered.groupby("user_guid")[
    "conversion_visit_timestamp"
].transform("max")
df_filtered = df_filtered[
    df_filtered["conversion_visit_timestamp"] == latest_timestamp_per_user
]
df_filtered = df_filtered.drop_duplicates()

# Delete main df to release memory
del df
gc.collect()

# Find conversion users
trial = trial_df[trial_df[transaction] == 1]
subscriber = sub_df[sub_df[transaction] == 1]
# registration = registration_df[registration_df[transaction] == 1]

# # Define a function to obtain median days to convert for each conversion type
def calculate_median_time_to_subscribe(
    stage_df, reference_df, ids, date_column, stage_name
):
    stage_min_date = stage_df.groupby(ids)[date_column].min()
    reference_min_date = reference_df.groupby(ids)[date_column].min()
    time_to_subscribe = (stage_min_date - reference_min_date).dt.days
    filtered_time_to_subscribe = time_to_subscribe[time_to_subscribe >= 1]
    median_days_to_subscribe = (
        filtered_time_to_subscribe.median()
        if not filtered_time_to_subscribe.empty
        else None
    )
    run_date = reference_df[date_column].max().normalize()
    return pd.DataFrame(
        {
            "stage": [stage_name],
            "median_days": [median_days_to_subscribe],
            "run_date": [run_date],
        }
    )


stages = [
    ("trial", trial, trial_df),
    ("subscriber", subscriber, sub_df)
    # ,("registration", registration, registration_df)
]

conversion_window_df = pd.concat(
    [
        calculate_median_time_to_subscribe(stage_df, ref_df, ids, date, stage_name)
        for stage_name, stage_df, ref_df in stages
    ],
    ignore_index=True,
)


################################################# Subscription: 90 days conversion window #########################################

attribution_df_all_subs_90 = pd.DataFrame()
normalized_removal_effects_all_subs_90 = pd.DataFrame()
markov_transition_matrix_all_subs_90 = pd.DataFrame()
user_df_all_subs_90 = pd.DataFrame()

end_date = datetime(2025, 1, 21).date()  
start_date = end_date - timedelta(days=90)

end_date = (
    pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
)
start_date = pd.to_datetime(start_date).tz_localize("UTC").normalize()


try:
    # Filter the DataFrame for the fixed 90-day period
    sub_df = sub_df[
        (sub_df[date] >= start_date) & (sub_df[date] <= end_date)
    ]  # filter to past 90 days data
    sub_df_max_date = sub_df[date].max().date()
    sub_df["user_max_date"] = sub_df.groupby("user_guid")[date].transform("max")
    sub_df[transaction] = 0
    sub_df.loc[sub_df[date] == sub_df["user_max_date"], transaction] = 1
    sub_df.drop(columns=["user_max_date"], inplace=True)
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
        decay_over_time=0.6, frequency=7
    )  # Frequency is in hours
    attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

    # Export user-level attribution data
    user_df_temp = attributions.as_pd_dataframe()

    # Calculate the number of touchpoints for each journey
    user_df_temp["num_touchpoints"] = (
        user_df_temp["channels_agg"].str.split(" > ").apply(len)
    )

    # Add a "run_date" column
    user_df_temp["run_date"] = sub_df_max_date

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

    # Add a "run_date" column
    markov_transition_matrix["run_date"] = sub_df_max_date

    # Append the result to the aggregated DataFrame with _90 suffix
    markov_transition_matrix_all_subs_90 = pd.concat(
        [markov_transition_matrix_all_subs_90, markov_transition_matrix],
        ignore_index=True,
    )

    # Process removal effects
    removal_effect_matrix = attribution_markov[3].round(3)

    # Assuming 'channel' is the index of the DataFrame
    channels = removal_effect_matrix.index

    # Extract the removal effect column as a DataFrame
    removal_effect_values = removal_effect_matrix[["removal_effect"]]

    # Normalize the values
    normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

    # Create a new DataFrame with the normalized values and 'channel' index
    normalized_removal_effects = pd.DataFrame(
        normalized_values, index=channels, columns=["removal_effect"]
    )

    # Add a "run_date" column with the maximum date
    normalized_removal_effects["run_date"] = sub_df_max_date

    # Add the original removal effects (before normalization) as a new column
    normalized_removal_effects[
        "removal_effect_raw"
    ] = removal_effect_values.values.flatten()

    # Reset the index to make 'channel' a regular column before saving to CSV
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

    # Add a "run_date" column with the maximum date
    attribution_df["run_date"] = sub_df_max_date

    # Ensure column names adhere to BigQuery's naming rules
    attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
    attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

    # Append the result to the aggregated DataFrame with _90 suffix
    attribution_df_all_subs_90 = pd.concat(
        [attribution_df_all_subs_90, attribution_df], ignore_index=True
    )

except Exception as e:
    print(
        f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}"
    )

attribution_df_all_subs_90["conversion_window"] = 90
normalized_removal_effects_all_subs_90["conversion_window"] = 90
markov_transition_matrix_all_subs_90["conversion_window"] = 90
user_df_all_subs_90["conversion_window"] = 90


################################################# Subscription: 60 days conversion window #########################################

attribution_df_all_subs_60 = pd.DataFrame()
normalized_removal_effects_all_subs_60 = pd.DataFrame()
markov_transition_matrix_all_subs_60 = pd.DataFrame()
user_df_all_subs_60 = pd.DataFrame()

end_date = datetime(2025, 1, 21).date()  
start_date = end_date - timedelta(days=60)

end_date = (
    pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
)
start_date = pd.to_datetime(start_date).tz_localize("UTC").normalize()

try:
    sub_df = sub_df[(sub_df[date] >= start_date) & (sub_df[date] <= end_date)]
    sub_df_max_date = sub_df[date].max().date()
    sub_df["user_max_date"] = sub_df.groupby("user_guid")[date].transform("max")
    sub_df[transaction] = 0
    sub_df.loc[sub_df[date] == sub_df["user_max_date"], transaction] = 1
    sub_df.drop(columns=["user_max_date"], inplace=True)
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
        decay_over_time=0.6, frequency=7
    )  # Frequency is in hours
    attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

    # Export user-level attribution data
    user_df_temp = attributions.as_pd_dataframe()

    # Calculate the number of touchpoints for each journey
    user_df_temp["num_touchpoints"] = (
        user_df_temp["channels_agg"].str.split(" > ").apply(len)
    )

    # Add a "run_date" column
    user_df_temp["run_date"] = sub_df_max_date

    # Append the result to the aggregated DataFrame with _60 suffix
    user_df_all_subs_60 = pd.concat(
        [user_df_all_subs_60, user_df_temp], ignore_index=True
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

    # Add a "run_date" column
    markov_transition_matrix["run_date"] = sub_df_max_date

    # Append the result to the aggregated DataFrame with _60 suffix
    markov_transition_matrix_all_subs_60 = pd.concat(
        [markov_transition_matrix_all_subs_60, markov_transition_matrix],
        ignore_index=True,
    )

    # Process removal effects
    removal_effect_matrix = attribution_markov[3].round(3)

    # Assuming 'channel' is the index of the DataFrame
    channels = removal_effect_matrix.index

    # Extract the removal effect column as a DataFrame
    removal_effect_values = removal_effect_matrix[["removal_effect"]]

    # Normalize the values
    normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

    # Create a new DataFrame with the normalized values and 'channel' index
    normalized_removal_effects = pd.DataFrame(
        normalized_values, index=channels, columns=["removal_effect"]
    )

    # Add a "run_date" column with the maximum date
    normalized_removal_effects["run_date"] = sub_df_max_date

    # Add the original removal effects (before normalization) as a new column
    normalized_removal_effects[
        "removal_effect_raw"
    ] = removal_effect_values.values.flatten()

    # Reset the index to make 'channel' a regular column before saving to CSV
    normalized_removal_effects.reset_index(inplace=True)

    # Rename the default 'index' column to 'channel'
    normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)

    # Append the result to the aggregated DataFrame with _60 suffix
    normalized_removal_effects_all_subs_60 = pd.concat(
        [normalized_removal_effects_all_subs_60, normalized_removal_effects],
        ignore_index=True,
    )

    # Process attribution by channels and models
    attribution_df = attributions.group_by_channels_models

    # Add a "run_date" column with the maximum date
    attribution_df["run_date"] = sub_df_max_date

    # Ensure column names adhere to BigQuery's naming rules
    attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
    attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

    # Append the result to the aggregated DataFrame with _60 suffix
    attribution_df_all_subs_60 = pd.concat(
        [attribution_df_all_subs_60, attribution_df], ignore_index=True
    )

except Exception as e:
    print(
        f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}"
    )

attribution_df_all_subs_60["conversion_window"] = 60
normalized_removal_effects_all_subs_60["conversion_window"] = 60
markov_transition_matrix_all_subs_60["conversion_window"] = 60
user_df_all_subs_60["conversion_window"] = 60

################################################# Subscription: 30 days conversion window #########################################

attribution_df_all_subs_30 = pd.DataFrame()
normalized_removal_effects_all_subs_30 = pd.DataFrame()
markov_transition_matrix_all_subs_30 = pd.DataFrame()
user_df_all_subs_30 = pd.DataFrame()

end_date = datetime(2025, 1, 21).date()  
start_date = end_date - timedelta(days=30)

end_date = (
    pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
)
start_date = pd.to_datetime(start_date).tz_localize("UTC").normalize()

try:
    sub_df = sub_df[(sub_df[date] >= start_date) & (sub_df[date] <= end_date)]
    sub_df_max_date = sub_df[date].max().date()
    sub_df["user_max_date"] = sub_df.groupby("user_guid")[date].transform("max")
    sub_df[transaction] = 0
    sub_df.loc[sub_df[date] == sub_df["user_max_date"], transaction] = 1
    sub_df.drop(columns=["user_max_date"], inplace=True)
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
        decay_over_time=0.6, frequency=7
    )  # Frequency is in hours
    attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

    # Export user-level attribution data
    user_df_temp = attributions.as_pd_dataframe()

    # Calculate the number of touchpoints for each journey
    user_df_temp["num_touchpoints"] = (
        user_df_temp["channels_agg"].str.split(" > ").apply(len)
    )

    # Add a "run_date" column
    user_df_temp["run_date"] = sub_df_max_date

    # Append the result to the aggregated DataFrame with _30 suffix
    user_df_all_subs_30 = pd.concat(
        [user_df_all_subs_30, user_df_temp], ignore_index=True
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

    # Add a "run_date" column
    markov_transition_matrix["run_date"] = sub_df_max_date

    # Append the result to the aggregated DataFrame with _30 suffix
    markov_transition_matrix_all_subs_30 = pd.concat(
        [markov_transition_matrix_all_subs_30, markov_transition_matrix],
        ignore_index=True,
    )

    # Process removal effects
    removal_effect_matrix = attribution_markov[3].round(3)

    # Assuming 'channel' is the index of the DataFrame
    channels = removal_effect_matrix.index

    # Extract the removal effect column as a DataFrame
    removal_effect_values = removal_effect_matrix[["removal_effect"]]

    # Normalize the values
    normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

    # Create a new DataFrame with the normalized values and 'channel' index
    normalized_removal_effects = pd.DataFrame(
        normalized_values, index=channels, columns=["removal_effect"]
    )

    # Add a "run_date" column with the maximum date
    normalized_removal_effects["run_date"] = sub_df_max_date

    # Add the original removal effects (before normalization) as a new column
    normalized_removal_effects[
        "removal_effect_raw"
    ] = removal_effect_values.values.flatten()

    # Reset the index to make 'channel' a regular column before saving to CSV
    normalized_removal_effects.reset_index(inplace=True)

    # Rename the default 'index' column to 'channel'
    normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)

    # Append the result to the aggregated DataFrame with _30 suffix
    normalized_removal_effects_all_subs_30 = pd.concat(
        [normalized_removal_effects_all_subs_30, normalized_removal_effects],
        ignore_index=True,
    )

    # Process attribution by channels and models
    attribution_df = attributions.group_by_channels_models

    # Add a "run_date" column with the maximum date
    attribution_df["run_date"] = sub_df_max_date

    # Ensure column names adhere to BigQuery's naming rules
    attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
    attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

    # Append the result to the aggregated DataFrame with _30 suffix
    attribution_df_all_subs_30 = pd.concat(
        [attribution_df_all_subs_30, attribution_df], ignore_index=True
    )

except Exception as e:
    print(
        f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}"
    )

# Add the conversion window identifier to each aggregated DataFrame
attribution_df_all_subs_30["conversion_window"] = 30
normalized_removal_effects_all_subs_30["conversion_window"] = 30
markov_transition_matrix_all_subs_30["conversion_window"] = 30
user_df_all_subs_30["conversion_window"] = 30


########################### Merge 3 conversion windows: subscription #############


suffixes = ["30", "60", "90"]

attribution_dfs = [
    globals()[f"attribution_df_all_subs_{suffix}"] for suffix in suffixes
]
attribution_df_all_subs = pd.concat(attribution_dfs, ignore_index=True)

removal_effects_dfs = [
    globals()[f"normalized_removal_effects_all_subs_{suffix}"] for suffix in suffixes
]
normalized_removal_effects_all_subs = pd.concat(removal_effects_dfs, ignore_index=True)

markov_transition_dfs = [
    globals()[f"markov_transition_matrix_all_subs_{suffix}"] for suffix in suffixes
]
markov_transition_matrix_all_subs = pd.concat(markov_transition_dfs, ignore_index=True)

user_dfs = [globals()[f"user_df_all_subs_{suffix}"] for suffix in suffixes]
user_df_all_subs = pd.concat(user_dfs, ignore_index=True)

user_df_all_subs["num_touchpoints"] = (
    user_df_all_subs["channels_agg"].str.split(" > ").apply(len)
)
user_df_all_subs["conversion_type"] = "Subscription"
markov_transition_matrix_all_subs["conversion_type"] = "Subscription"
normalized_removal_effects_all_subs["conversion_type"] = "Subscription"
attribution_df_all_subs["conversion_type"] = "Subscription"


################################################# Trial: 90 days conversion window #########################################

attribution_df_all_trial_90 = pd.DataFrame()
normalized_removal_effects_all_trial_90 = pd.DataFrame()
markov_transition_matrix_all_trial_90 = pd.DataFrame()
user_df_all_trial_90 = pd.DataFrame()

end_date = datetime(2025, 1, 21).date()  
start_date = end_date - timedelta(days=90)

end_date = (
    pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
)
start_date = pd.to_datetime(start_date).tz_localize("UTC").normalize()

try:
    trial_df = trial_df[(trial_df[date] >= start_date) & (trial_df[date] <= end_date)]
    trial_df_max_date = trial_df[date].max().date()
    trial_df["user_max_date"] = trial_df.groupby("user_guid")[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[trial_df[date] == trial_df["user_max_date"], transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

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

    # Apply various attribution models
    attributions.attribution_last_click()
    attributions.attribution_first_click()
    attributions.attribution_position_based(
        list_positions_first_middle_last=[0.3, 0.3, 0.4]
    )
    attributions.attribution_time_decay(
        decay_over_time=0.6, frequency=7
    )  # Frequency is in hours
    attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

    # Export user-level attribution data
    user_df_temp = attributions.as_pd_dataframe()

    # Calculate the number of touchpoints for each journey
    user_df_temp["num_touchpoints"] = (
        user_df_temp["channels_agg"].str.split(" > ").apply(len)
    )

    # Add a "run_date" column
    user_df_temp["run_date"] = trial_df_max_date

    # Append the result to the aggregated DataFrame with _90 suffix
    user_df_all_trial_90 = pd.concat(
        [user_df_all_trial_90, user_df_temp], ignore_index=True
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

    # Add a "run_date" column
    markov_transition_matrix["run_date"] = trial_df_max_date

    # Append the result to the aggregated DataFrame with _90 suffix
    markov_transition_matrix_all_trial_90 = pd.concat(
        [markov_transition_matrix_all_trial_90, markov_transition_matrix],
        ignore_index=True,
    )

    # Process removal effects
    removal_effect_matrix = attribution_markov[3].round(3)

    # Assuming 'channel' is the index of the DataFrame
    channels = removal_effect_matrix.index

    # Extract the removal effect column as a DataFrame
    removal_effect_values = removal_effect_matrix[["removal_effect"]]

    # Normalize the values
    normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

    # Create a new DataFrame with the normalized values and 'channel' index
    normalized_removal_effects = pd.DataFrame(
        normalized_values, index=channels, columns=["removal_effect"]
    )

    # Add a "run_date" column with the maximum date
    normalized_removal_effects["run_date"] = trial_df_max_date

    # Add the original removal effects (before normalization) as a new column
    normalized_removal_effects[
        "removal_effect_raw"
    ] = removal_effect_values.values.flatten()

    # Reset the index to make 'channel' a regular column before saving to CSV
    normalized_removal_effects.reset_index(inplace=True)

    # Rename the default 'index' column to 'channel'
    normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)

    # Append the result to the aggregated DataFrame with _90 suffix
    normalized_removal_effects_all_trial_90 = pd.concat(
        [normalized_removal_effects_all_trial_90, normalized_removal_effects],
        ignore_index=True,
    )

    # Process attribution by channels and models
    attribution_df = attributions.group_by_channels_models

    # Add a "run_date" column with the maximum date
    attribution_df["run_date"] = trial_df_max_date

    # Ensure column names adhere to BigQuery's naming rules
    attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
    attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

    # Append the result to the aggregated DataFrame with _90 suffix
    attribution_df_all_trial_90 = pd.concat(
        [attribution_df_all_trial_90, attribution_df], ignore_index=True
    )

except Exception as e:
    print(
        f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}"
    )

attribution_df_all_trial_90["conversion_window"] = 90
normalized_removal_effects_all_trial_90["conversion_window"] = 90
markov_transition_matrix_all_trial_90["conversion_window"] = 90
user_df_all_trial_90["conversion_window"] = 90


################################################# Trial: 60 days conversion window #########################################

attribution_df_all_trial_60 = pd.DataFrame()
normalized_removal_effects_all_trial_60 = pd.DataFrame()
markov_transition_matrix_all_trial_60 = pd.DataFrame()
user_df_all_trial_60 = pd.DataFrame()

end_date = datetime(2025, 1, 21).date()  
start_date = end_date - timedelta(days=60)

end_date = (
    pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
)
start_date = pd.to_datetime(start_date).tz_localize("UTC").normalize()

try:
    trial_df = trial_df[(trial_df[date] >= start_date) & (trial_df[date] <= end_date)]
    trial_df_max_date = trial_df[date].max().date()
    trial_df["user_max_date"] = trial_df.groupby("user_guid")[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[trial_df[date] == trial_df["user_max_date"], transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

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

    # Apply various attribution models
    attributions.attribution_last_click()
    attributions.attribution_first_click()
    attributions.attribution_position_based(
        list_positions_first_middle_last=[0.3, 0.3, 0.4]
    )
    attributions.attribution_time_decay(
        decay_over_time=0.6, frequency=7
    )  # Frequency is in hours
    attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

    # Export user-level attribution data
    user_df_temp = attributions.as_pd_dataframe()

    # Calculate the number of touchpoints for each journey
    user_df_temp["num_touchpoints"] = (
        user_df_temp["channels_agg"].str.split(" > ").apply(len)
    )

    # Add a "run_date" column
    user_df_temp["run_date"] = trial_df_max_date

    # Append the result to the aggregated DataFrame with _60 suffix
    user_df_all_trial_60 = pd.concat(
        [user_df_all_trial_60, user_df_temp], ignore_index=True
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

    # Add a "run_date" column
    markov_transition_matrix["run_date"] = trial_df_max_date

    # Append the result to the aggregated DataFrame with _60 suffix
    markov_transition_matrix_all_trial_60 = pd.concat(
        [markov_transition_matrix_all_trial_60, markov_transition_matrix],
        ignore_index=True,
    )

    # Process removal effects
    removal_effect_matrix = attribution_markov[3].round(3)

    # Assuming 'channel' is the index of the DataFrame
    channels = removal_effect_matrix.index

    # Extract the removal effect column as a DataFrame
    removal_effect_values = removal_effect_matrix[["removal_effect"]]

    # Normalize the values
    normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

    # Create a new DataFrame with the normalized values and 'channel' index
    normalized_removal_effects = pd.DataFrame(
        normalized_values, index=channels, columns=["removal_effect"]
    )

    # Add a "run_date" column with the maximum date
    normalized_removal_effects["run_date"] = trial_df_max_date

    # Add the original removal effects (before normalization) as a new column
    normalized_removal_effects[
        "removal_effect_raw"
    ] = removal_effect_values.values.flatten()

    # Reset the index to make 'channel' a regular column before saving to CSV
    normalized_removal_effects.reset_index(inplace=True)

    # Rename the default 'index' column to 'channel'
    normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)

    # Append the result to the aggregated DataFrame with _60 suffix
    normalized_removal_effects_all_trial_60 = pd.concat(
        [normalized_removal_effects_all_trial_60, normalized_removal_effects],
        ignore_index=True,
    )

    # Process attribution by channels and models
    attribution_df = attributions.group_by_channels_models

    # Add a "run_date" column with the maximum date
    attribution_df["run_date"] = trial_df_max_date

    # Ensure column names adhere to BigQuery's naming rules
    attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
    attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

    # Append the result to the aggregated DataFrame with _60 suffix
    attribution_df_all_trial_60 = pd.concat(
        [attribution_df_all_trial_60, attribution_df], ignore_index=True
    )

except Exception as e:
    print(
        f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}"
    )

attribution_df_all_trial_60["conversion_window"] = 60
normalized_removal_effects_all_trial_60["conversion_window"] = 60
markov_transition_matrix_all_trial_60["conversion_window"] = 60
user_df_all_trial_60["conversion_window"] = 60


################################################# Trial: 30 days conversion window #########################################

attribution_df_all_trial_30 = pd.DataFrame()
normalized_removal_effects_all_trial_30 = pd.DataFrame()
markov_transition_matrix_all_trial_30 = pd.DataFrame()
user_df_all_trial_30 = pd.DataFrame()

end_date = datetime(2025, 1, 21).date()  
start_date = end_date - timedelta(days=30)

end_date = (
    pd.to_datetime(end_date).tz_localize("UTC").replace(hour=23, minute=59, second=59)
)
start_date = pd.to_datetime(start_date).tz_localize("UTC").normalize()

try:
    trial_df = trial_df[(trial_df[date] >= start_date) & (trial_df[date] <= end_date)]
    trial_df_max_date = trial_df[date].max().date()
    trial_df["user_max_date"] = trial_df.groupby("user_guid")[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[trial_df[date] == trial_df["user_max_date"], transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

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

    # Apply various attribution models
    attributions.attribution_last_click()
    attributions.attribution_first_click()
    attributions.attribution_position_based(
        list_positions_first_middle_last=[0.3, 0.3, 0.4]
    )
    attributions.attribution_time_decay(
        decay_over_time=0.6, frequency=7
    )  # Frequency is in hours
    attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

    # Export user-level attribution data
    user_df_temp = attributions.as_pd_dataframe()

    # Calculate the number of touchpoints for each journey
    user_df_temp["num_touchpoints"] = (
        user_df_temp["channels_agg"].str.split(" > ").apply(len)
    )

    # Add a "run_date" column
    user_df_temp["run_date"] = trial_df_max_date

    # Append the result to the aggregated DataFrame with _30 suffix
    user_df_all_trial_30 = pd.concat(
        [user_df_all_trial_30, user_df_temp], ignore_index=True
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

    # Add a "run_date" column
    markov_transition_matrix["run_date"] = trial_df_max_date

    # Append the result to the aggregated DataFrame with _60 suffix
    markov_transition_matrix_all_trial_30 = pd.concat(
        [markov_transition_matrix_all_trial_30, markov_transition_matrix],
        ignore_index=True,
    )

    # Process removal effects
    removal_effect_matrix = attribution_markov[3].round(3)

    # Assuming 'channel' is the index of the DataFrame
    channels = removal_effect_matrix.index

    # Extract the removal effect column as a DataFrame
    removal_effect_values = removal_effect_matrix[["removal_effect"]]

    # Normalize the values
    normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

    # Create a new DataFrame with the normalized values and 'channel' index
    normalized_removal_effects = pd.DataFrame(
        normalized_values, index=channels, columns=["removal_effect"]
    )

    # Add a "run_date" column with the maximum date
    normalized_removal_effects["run_date"] = trial_df_max_date

    # Add the original removal effects (before normalization) as a new column
    normalized_removal_effects[
        "removal_effect_raw"
    ] = removal_effect_values.values.flatten()

    # Reset the index to make 'channel' a regular column before saving to CSV
    normalized_removal_effects.reset_index(inplace=True)

    # Rename the default 'index' column to 'channel'
    normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)

    # Append the result to the aggregated DataFrame with _30 suffix
    normalized_removal_effects_all_trial_30 = pd.concat(
        [normalized_removal_effects_all_trial_30, normalized_removal_effects],
        ignore_index=True,
    )

    # Process attribution by channels and models
    attribution_df = attributions.group_by_channels_models

    # Add a "run_date" column with the maximum date
    attribution_df["run_date"] = trial_df_max_date

    # Ensure column names adhere to BigQuery's naming rules
    attribution_df.columns = attribution_df.columns.str.replace(".", "_", regex=False)
    attribution_df.columns = attribution_df.columns.str.replace(" ", "_", regex=False)

    # Append the result to the aggregated DataFrame with _60 suffix
    attribution_df_all_trial_30 = pd.concat(
        [attribution_df_all_trial_30, attribution_df], ignore_index=True
    )

except Exception as e:
    print(
        f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}"
    )


attribution_df_all_trial_30["conversion_window"] = 30
normalized_removal_effects_all_trial_30["conversion_window"] = 30
markov_transition_matrix_all_trial_30["conversion_window"] = 30
user_df_all_trial_30["conversion_window"] = 30


suffixes = ["30", "60", "90"]

attribution_dfs_trial = [
    globals()[f"attribution_df_all_trial_{suffix}"] for suffix in suffixes
]
attribution_df_all_trial = pd.concat(attribution_dfs_trial, ignore_index=True)

removal_effects_dfs_trial = [
    globals()[f"normalized_removal_effects_all_trial_{suffix}"] for suffix in suffixes
]
normalized_removal_effects_all_trial = pd.concat(
    removal_effects_dfs_trial, ignore_index=True
)

markov_transition_dfs_trial = [
    globals()[f"markov_transition_matrix_all_trial_{suffix}"] for suffix in suffixes
]
markov_transition_matrix_all_trial = pd.concat(
    markov_transition_dfs_trial, ignore_index=True
)

user_dfs_trial = [globals()[f"user_df_all_trial_{suffix}"] for suffix in suffixes]
user_df_all_trial = pd.concat(user_dfs_trial, ignore_index=True)


user_df_all_trial["num_touchpoints"] = (
    user_df_all_trial["channels_agg"].str.split(" > ").apply(len)
)
user_df_all_trial["conversion_type"] = "Trial"
markov_transition_matrix_all_trial["conversion_type"] = "Trial"
normalized_removal_effects_all_trial["conversion_type"] = "Trial"
attribution_df_all_trial["conversion_type"] = "Trial"


################################################ Registration: 90 days conversion window #########################################

# attribution_df_all_registration_90 = pd.DataFrame()
# normalized_removal_effects_all_registration_90 = pd.DataFrame()
# markov_transition_matrix_all_registration_90 = pd.DataFrame()
# user_df_all_registration_90 = pd.DataFrame()

# end_date = (datetime.now() - timedelta(days=1)).date()  # Exclude today
# start_date = (end_date - timedelta(days=90))

# end_date = pd.to_datetime(end_date).tz_localize('UTC').replace(hour=23, minute=59, second=59)
# start_date = pd.to_datetime(start_date).tz_localize('UTC').normalize()

# try:
#     registration_df = registration_df[(registration_df[date] >= start_date) & (registration_df[date] <= end_date)]
#     registration_df_max_date = registration_df[date].max().date()
#     registration_df['user_max_date'] = registration_df.groupby('user_guid')[date].transform('max')
#     registration_df[transaction] = 0
#     registration_df.loc[registration_df[date] == registration_df['user_max_date'], transaction] = 1
#     registration_df.drop(columns=['user_max_date'], inplace=True)
#     registration_df = registration_df.sort_values([ids, date], ascending=[False, True])

#     # Initialize the MAM class
#     attributions = MAM(
#         registration_df,
#         group_channels=True,
#         channels_colname=touchpoint,
#         journey_with_conv_colname=transaction,
#         group_channels_by_id_list=[ids],
#         group_timestamp_colname=date,
#         create_journey_id_based_on_conversion=True
#     )

#     # Apply various attribution models
#     attributions.attribution_last_click()
#     attributions.attribution_first_click()
#     attributions.attribution_position_based(list_positions_first_middle_last=[0.3, 0.3, 0.4])
#     attributions.attribution_time_decay(decay_over_time=0.6, frequency=7)  # Frequency is in hours
#     attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

#     # Export user-level attribution data
#     user_df_temp = attributions.as_pd_dataframe()

#     # Calculate the number of touchpoints for each journey
#     user_df_temp['num_touchpoints'] = user_df_temp['channels_agg'].str.split(' > ').apply(len)

#     # Add a "run_date" column
#     user_df_temp['run_date'] = registration_df_max_date

#     # Append the result to the aggregated DataFrame with _90 suffix
#     user_df_all_registration_90 = pd.concat([user_df_all_registration_90, user_df_temp], ignore_index=True)

#     ##################################

#     # Process Markov transition matrix
#     markov_transition_matrix = attribution_markov[2].round(3)
#     markov_transition_matrix = markov_transition_matrix.rename(
#         index=lambda x: x.replace("(inicio)", "(start)"),
#         columns=lambda x: x.replace("(inicio)", "(start)")
#     )

#     # Reset index to convert the index (pages) to a column
#     markov_transition_matrix.reset_index(inplace=True)

#     # Melt the DataFrame to convert columns to rows
#     markov_transition_matrix = pd.melt(
#         markov_transition_matrix,
#         id_vars='index',
#         var_name='destination',
#         value_name='probability'
#     )

#     # Rename columns
#     markov_transition_matrix.columns = ['source', 'destination', 'probability']

#     # Add a "run_date" column
#     markov_transition_matrix['run_date'] = registration_df_max_date

#     # Append the result to the aggregated DataFrame with _90 suffix
#     markov_transition_matrix_all_registration_90 = pd.concat(
#         [markov_transition_matrix_all_registration_90, markov_transition_matrix], ignore_index=True
#     )

#     # Process removal effects
#     removal_effect_matrix = attribution_markov[3].round(3)

#     # Assuming 'channel' is the index of the DataFrame
#     channels = removal_effect_matrix.index

#     # Extract the removal effect column as a DataFrame
#     removal_effect_values = removal_effect_matrix[['removal_effect']]

#     # Normalize the values
#     normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

#     # Create a new DataFrame with the normalized values and 'channel' index
#     normalized_removal_effects = pd.DataFrame(
#         normalized_values, index=channels, columns=['removal_effect']
#     )

#     # Add a "run_date" column with the maximum date
#     normalized_removal_effects['run_date'] = registration_df_max_date

#     # Add the original removal effects (before normalization) as a new column
#     normalized_removal_effects['removal_effect_raw'] = removal_effect_values.values.flatten()

#     # Reset the index to make 'channel' a regular column before saving to CSV
#     normalized_removal_effects.reset_index(inplace=True)

#     # Rename the default 'index' column to 'channel'
#     normalized_removal_effects.rename(columns={'index': 'channel'}, inplace=True)

#     # Append the result to the aggregated DataFrame with _90 suffix
#     normalized_removal_effects_all_registration_90 = pd.concat(
#         [normalized_removal_effects_all_registration_90, normalized_removal_effects], ignore_index=True
#     )

#     # Process attribution by channels and models
#     attribution_df = attributions.group_by_channels_models

#     # Add a "run_date" column with the maximum date
#     attribution_df['run_date'] = registration_df_max_date

#     # Ensure column names adhere to BigQuery's naming rules
#     attribution_df.columns = attribution_df.columns.str.replace('.', '_', regex=False)
#     attribution_df.columns = attribution_df.columns.str.replace(' ', '_', regex=False)

#     # Append the result to the aggregated DataFrame with _90 suffix
#     attribution_df_all_registration_90 = pd.concat([attribution_df_all_registration_90, attribution_df], ignore_index=True)

# except Exception as e:
#     print(f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}")

# attribution_df_all_registration_90['conversion_window'] = 90
# normalized_removal_effects_all_registration_90['conversion_window'] = 90
# markov_transition_matrix_all_registration_90['conversion_window'] = 90
# user_df_all_registration_90['conversion_window'] = 90

# ################################################# Registration: 60 days conversion window #########################################

# attribution_df_all_registration_60 = pd.DataFrame()
# normalized_removal_effects_all_registration_60 = pd.DataFrame()
# markov_transition_matrix_all_registration_60 = pd.DataFrame()
# user_df_all_registration_60 = pd.DataFrame()

# end_date = (datetime.now() - timedelta(days=1)).date()  # Exclude today
# start_date = (end_date - timedelta(days=60))

# end_date = pd.to_datetime(end_date).tz_localize('UTC').replace(hour=23, minute=59, second=59)
# start_date = pd.to_datetime(start_date).tz_localize('UTC').normalize()
# try:
#     registration_df = registration_df[(registration_df[date] >= start_date) & (registration_df[date] <= end_date)]
#     registration_df_max_date = registration_df[date].max().date()
#     registration_df['user_max_date'] = registration_df.groupby('user_guid')[date].transform('max')
#     registration_df[transaction] = 0
#     registration_df.loc[registration_df[date] == registration_df['user_max_date'], transaction] = 1
#     registration_df.drop(columns=['user_max_date'], inplace=True)
#     registration_df = registration_df.sort_values([ids, date], ascending=[False, True])

#     # Initialize the MAM class
#     attributions = MAM(
#         registration_df,
#         group_channels=True,
#         channels_colname=touchpoint,
#         journey_with_conv_colname=transaction,
#         group_channels_by_id_list=[ids],
#         group_timestamp_colname=date,
#         create_journey_id_based_on_conversion=True
#     )

#     # Apply various attribution models
#     attributions.attribution_last_click()
#     attributions.attribution_first_click()
#     attributions.attribution_position_based(list_positions_first_middle_last=[0.3, 0.3, 0.4])
#     attributions.attribution_time_decay(decay_over_time=0.6, frequency=7)  # Frequency is in hours
#     attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

#     # Export user-level attribution data
#     user_df_temp = attributions.as_pd_dataframe()

#     # Calculate the number of touchpoints for each journey
#     user_df_temp['num_touchpoints'] = user_df_temp['channels_agg'].str.split(' > ').apply(len)

#     # Add a "run_date" column
#     user_df_temp['run_date'] = registration_df_max_date

#     # Append the result to the aggregated DataFrame with _90 suffix
#     user_df_all_registration_60 = pd.concat([user_df_all_registration_60, user_df_temp], ignore_index=True)

#     ##################################

#     # Process Markov transition matrix
#     markov_transition_matrix = attribution_markov[2].round(3)
#     markov_transition_matrix = markov_transition_matrix.rename(
#         index=lambda x: x.replace("(inicio)", "(start)"),
#         columns=lambda x: x.replace("(inicio)", "(start)")
#     )

#     # Reset index to convert the index (pages) to a column
#     markov_transition_matrix.reset_index(inplace=True)

#     # Melt the DataFrame to convert columns to rows
#     markov_transition_matrix = pd.melt(
#         markov_transition_matrix,
#         id_vars='index',
#         var_name='destination',
#         value_name='probability'
#     )

#     # Rename columns
#     markov_transition_matrix.columns = ['source', 'destination', 'probability']

#     # Add a "run_date" column
#     markov_transition_matrix['run_date'] = registration_df_max_date

#     # Append the result to the aggregated DataFrame with _90 suffix
#     markov_transition_matrix_all_registration_60 = pd.concat(
#         [markov_transition_matrix_all_registration_60, markov_transition_matrix], ignore_index=True
#     )

#     # Process removal effects
#     removal_effect_matrix = attribution_markov[3].round(3)

#     # Assuming 'channel' is the index of the DataFrame
#     channels = removal_effect_matrix.index

#     # Extract the removal effect column as a DataFrame
#     removal_effect_values = removal_effect_matrix[['removal_effect']]

#     # Normalize the values
#     normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

#     # Create a new DataFrame with the normalized values and 'channel' index
#     normalized_removal_effects = pd.DataFrame(
#         normalized_values, index=channels, columns=['removal_effect']
#     )

#     # Add a "run_date" column with the maximum date
#     normalized_removal_effects['run_date'] = registration_df_max_date

#     # Add the original removal effects (before normalization) as a new column
#     normalized_removal_effects['removal_effect_raw'] = removal_effect_values.values.flatten()

#     # Reset the index to make 'channel' a regular column before saving to CSV
#     normalized_removal_effects.reset_index(inplace=True)

#     # Rename the default 'index' column to 'channel'
#     normalized_removal_effects.rename(columns={'index': 'channel'}, inplace=True)

#     # Append the result to the aggregated DataFrame with _60 suffix
#     normalized_removal_effects_all_registration_60 = pd.concat(
#         [normalized_removal_effects_all_registration_60, normalized_removal_effects], ignore_index=True
#     )

#     # Process attribution by channels and models
#     attribution_df = attributions.group_by_channels_models

#     # Add a "run_date" column with the maximum date
#     attribution_df['run_date'] = registration_df_max_date

#     # Ensure column names adhere to BigQuery's naming rules
#     attribution_df.columns = attribution_df.columns.str.replace('.', '_', regex=False)
#     attribution_df.columns = attribution_df.columns.str.replace(' ', '_', regex=False)

#     # Append the result to the aggregated DataFrame with _90 suffix
#     attribution_df_all_registration_60 = pd.concat([attribution_df_all_registration_60, attribution_df], ignore_index=True)

# except Exception as e:
#     print(f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}")

# attribution_df_all_registration_60['conversion_window'] = 60
# normalized_removal_effects_all_registration_60['conversion_window'] = 60
# markov_transition_matrix_all_registration_60['conversion_window'] = 60
# user_df_all_registration_60['conversion_window'] = 60

# ################################################# Registration: 30 days conversion window #########################################

# attribution_df_all_registration_30 = pd.DataFrame()
# normalized_removal_effects_all_registration_30 = pd.DataFrame()
# markov_transition_matrix_all_registration_30 = pd.DataFrame()
# user_df_all_registration_30 = pd.DataFrame()

# end_date = (datetime.now() - timedelta(days=1)).date()  # Exclude today
# start_date = (end_date - timedelta(days=30))

# end_date = pd.to_datetime(end_date).tz_localize('UTC').replace(hour=23, minute=59, second=59)
# start_date = pd.to_datetime(start_date).tz_localize('UTC').normalize()

# try:
#     registration_df = registration_df[(registration_df[date] >= start_date) & (registration_df[date] <= end_date)]
#     registration_df_max_date = registration_df[date].max().date()
#     registration_df['user_max_date'] = registration_df.groupby('user_guid')[date].transform('max')
#     registration_df[transaction] = 0
#     registration_df.loc[registration_df[date] == registration_df['user_max_date'], transaction] = 1
#     registration_df.drop(columns=['user_max_date'], inplace=True)
#     registration_df = registration_df.sort_values([ids, date], ascending=[False, True])

#     # Initialize the MAM class
#     attributions = MAM(
#         registration_df,
#         group_channels=True,
#         channels_colname=touchpoint,
#         journey_with_conv_colname=transaction,
#         group_channels_by_id_list=[ids],
#         group_timestamp_colname=date,
#         create_journey_id_based_on_conversion=True
#     )

#     # Apply various attribution models
#     attributions.attribution_last_click()
#     attributions.attribution_first_click()
#     attributions.attribution_position_based(list_positions_first_middle_last=[0.3, 0.3, 0.4])
#     attributions.attribution_time_decay(decay_over_time=0.6, frequency=7)  # Frequency is in hours
#     attribution_markov = attributions.attribution_markov(transition_to_same_state=False)

#     # Export user-level attribution data
#     user_df_temp = attributions.as_pd_dataframe()

#     # Calculate the number of touchpoints for each journey
#     user_df_temp['num_touchpoints'] = user_df_temp['channels_agg'].str.split(' > ').apply(len)

#     # Add a "run_date" column
#     user_df_temp['run_date'] = registration_df_max_date

#     # Append the result to the aggregated DataFrame with _30 suffix
#     user_df_all_registration_30 = pd.concat([user_df_all_registration_30, user_df_temp], ignore_index=True)

#     ##################################

#     # Process Markov transition matrix
#     markov_transition_matrix = attribution_markov[2].round(3)
#     markov_transition_matrix = markov_transition_matrix.rename(
#         index=lambda x: x.replace("(inicio)", "(start)"),
#         columns=lambda x: x.replace("(inicio)", "(start)")
#     )

#     # Reset index to convert the index (pages) to a column
#     markov_transition_matrix.reset_index(inplace=True)

#     # Melt the DataFrame to convert columns to rows
#     markov_transition_matrix = pd.melt(
#         markov_transition_matrix,
#         id_vars='index',
#         var_name='destination',
#         value_name='probability'
#     )

#     # Rename columns
#     markov_transition_matrix.columns = ['source', 'destination', 'probability']

#     # Add a "run_date" column
#     markov_transition_matrix['run_date'] = registration_df_max_date

#     # Append the result to the aggregated DataFrame with _30 suffix
#     markov_transition_matrix_all_registration_30 = pd.concat(
#         [markov_transition_matrix_all_registration_30, markov_transition_matrix], ignore_index=True
#     )

#     # Process removal effects
#     removal_effect_matrix = attribution_markov[3].round(3)

#     # Assuming 'channel' is the index of the DataFrame
#     channels = removal_effect_matrix.index

#     # Extract the removal effect column as a DataFrame
#     removal_effect_values = removal_effect_matrix[['removal_effect']]

#     # Normalize the values
#     normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100

#     # Create a new DataFrame with the normalized values and 'channel' index
#     normalized_removal_effects = pd.DataFrame(
#         normalized_values, index=channels, columns=['removal_effect']
#     )

#     # Add a "run_date" column with the maximum date
#     normalized_removal_effects['run_date'] = registration_df_max_date

#     # Add the original removal effects (before normalization) as a new column
#     normalized_removal_effects['removal_effect_raw'] = removal_effect_values.values.flatten()

#     # Reset the index to make 'channel' a regular column before saving to CSV
#     normalized_removal_effects.reset_index(inplace=True)

#     # Rename the default 'index' column to 'channel'
#     normalized_removal_effects.rename(columns={'index': 'channel'}, inplace=True)

#     # Append the result to the aggregated DataFrame with _60 suffix
#     normalized_removal_effects_all_registration_30 = pd.concat(
#         [normalized_removal_effects_all_registration_30, normalized_removal_effects], ignore_index=True
#     )

#     # Process attribution by channels and models
#     attribution_df = attributions.group_by_channels_models

#     # Add a "run_date" column with the maximum date
#     attribution_df['run_date'] = registration_df_max_date

#     # Ensure column names adhere to BigQuery's naming rules
#     attribution_df.columns = attribution_df.columns.str.replace('.', '_', regex=False)
#     attribution_df.columns = attribution_df.columns.str.replace(' ', '_', regex=False)

#     # Append the result to the aggregated DataFrame with _90 suffix
#     attribution_df_all_registration_30 = pd.concat([attribution_df_all_registration_30, attribution_df], ignore_index=True)

# except Exception as e:
#     print(f"An error occurred for the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}: {e}")

# attribution_df_all_registration_30['conversion_window'] = 30
# normalized_removal_effects_all_registration_30['conversion_window'] = 30
# markov_transition_matrix_all_registration_30['conversion_window'] = 30
# user_df_all_registration_30['conversion_window'] = 30

# suffixes = ['30', '60', '90']

# attribution_dfs_registration = [globals()[f'attribution_df_all_registration_{suffix}'] for suffix in suffixes]
# attribution_df_all_registration = pd.concat(attribution_dfs_registration, ignore_index=True)

# removal_effects_dfs_registration = [globals()[f'normalized_removal_effects_all_registration_{suffix}'] for suffix in suffixes]
# normalized_removal_effects_all_registration = pd.concat(removal_effects_dfs_registration, ignore_index=True)

# markov_transition_dfs_registration = [globals()[f'markov_transition_matrix_all_registration_{suffix}'] for suffix in suffixes]
# markov_transition_matrix_all_registration = pd.concat(markov_transition_dfs_registration, ignore_index=True)

# user_dfs_registration = [globals()[f'user_df_all_registration_{suffix}'] for suffix in suffixes]
# user_df_all_registration = pd.concat(user_dfs_registration, ignore_index=True)


# user_df_all_registration['num_touchpoints'] = user_df_all_registration['channels_agg'].str.split(' > ').apply(len)
# user_df_all_registration["conversion_type"] = "Registration"
# markov_transition_matrix_all_registration["conversion_type"] = "Registration"
# normalized_removal_effects_all_registration["conversion_type"] = "Registration"
# attribution_df_all_registration["conversion_type"] = "Registration"


################################################# Merge Trial, Subscription, Registration subsets #########################################

user_df_all = pd.concat([user_df_all_trial, user_df_all_subs], ignore_index=True)
# user_df_all= pd.concat([user_df_all, user_df_all_registration], ignore_index=True)

markov_transition_matrix_all = pd.concat(
    [markov_transition_matrix_all_trial, markov_transition_matrix_all_subs],
    ignore_index=True,
)
# markov_transition_matrix_all = pd.concat([markov_transition_matrix_all, markov_transition_matrix_all_registration], ignore_index=True)

normalized_removal_effects_all = pd.concat(
    [normalized_removal_effects_all_trial, normalized_removal_effects_all_subs],
    ignore_index=True,
)
# normalized_removal_effects_all = pd.concat([normalized_removal_effects_all, normalized_removal_effects_all_registration], ignore_index=True)

attribution_df_all = pd.concat(
    [attribution_df_all_subs, attribution_df_all_trial], ignore_index=True
)
# attribution_df_all = pd.concat([attribution_df_all, attribution_df_all_registration], ignore_index=True)

# Rename user_df_all columns in big query format
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


############################################ Merge with LTV #########################################

client = bigquery.Client(project="ft-customer-analytics")
ltv_table_id = "ft-customer-analytics.crg_nniu.ltv_last_90_days"
query = f"""
    SELECT * FROM
        {ltv_table_id}
"""


query_job = client.query(query)
ltv_df = query_job.to_dataframe()

ltv_df = ltv_df.dropna(subset=["ltv_acquisition_capped_12m"])

group_columns = [col for col in ltv_df.columns if col != "ltv_acquisition_capped_12m"]

# Group by all columns except 'ltv_acquisition_capped_12m' and calculate its mean
ltv_df = ltv_df.groupby(group_columns, as_index=False).agg(
    ltv_acquisition_capped_12m=("ltv_acquisition_capped_12m", "mean")
)

# # extract user guid from journey id
user_df_all["user_guid"] = user_df_all["journey_id"].str.extract(r"id:(.*)_J:0")[0]

# date column conversion for ltv df
ltv_df["product_order_timestamp"] = pd.to_datetime(
    ltv_df["product_order_timestamp"], utc=True
)
user_df_all["run_date"] = pd.to_datetime(user_df_all["run_date"], utc=True)

# Convert date columns
ltv_df["product_order_timestamp"] = ltv_df["product_order_timestamp"].dt.date
user_df_all["run_date"] = user_df_all["run_date"].dt.date

# convert ltv 12m as float
ltv_df["ltv_acquisition_capped_12m"] = ltv_df["ltv_acquisition_capped_12m"].astype(
    float
)

# Merge final user_df_all with country, user status, product type columns
user_df_all = pd.merge(
    user_df_all, df_filtered, how="left", left_on=["user_guid"], right_on=["user_guid"]
)

user_df_all = user_df_all[user_df_all["conversion_value"] == 1]
user_df_all["product_arrangement_id"] = user_df_all["product_arrangement_id"].fillna(0)

user_df_all = pd.merge(
    user_df_all,
    ltv_df,
    left_on=["product_arrangement_id", "run_date"],
    right_on=["product_arrangement_id", "product_order_timestamp"],
    how="left",
)

# Trial conversion window DataFrames
user_df_trial_30 = user_df_all[
    (user_df_all["conversion_window"] == 30)
    & (user_df_all["conversion_type"] == "Trial")
]
user_df_trial_60 = user_df_all[
    (user_df_all["conversion_window"] == 60)
    & (user_df_all["conversion_type"] == "Trial")
]
user_df_trial_90 = user_df_all[
    (user_df_all["conversion_window"] == 90)
    & (user_df_all["conversion_type"] == "Trial")
]

# Subscription conversion window DataFrames
user_df_subscription_30 = user_df_all[
    (user_df_all["conversion_window"] == 30)
    & (user_df_all["conversion_type"] == "Subscription")
]
user_df_subscription_60 = user_df_all[
    (user_df_all["conversion_window"] == 60)
    & (user_df_all["conversion_type"] == "Subscription")
]
user_df_subscription_90 = user_df_all[
    (user_df_all["conversion_window"] == 90)
    & (user_df_all["conversion_type"] == "Subscription")
]

# Registration conversion window DataFrames
user_df_registration_30 = user_df_all[
    (user_df_all["conversion_window"] == 30)
    & (user_df_all["conversion_type"] == "Registration")
]
user_df_registration_60 = user_df_all[
    (user_df_all["conversion_window"] == 60)
    & (user_df_all["conversion_type"] == "Registration")
]
user_df_registration_90 = user_df_all[
    (user_df_all["conversion_window"] == 90)
    & (user_df_all["conversion_type"] == "Registration")
]


def calculate_removal_effect(row):
    attr = row["attribution_markov_algorithmic"]
    ltv = row["ltv_acquisition_capped_12m"]
    channels = row["channels_agg"]

    if pd.isna(attr) or pd.isna(channels):
        return np.nan

    attr_parts = attr.split(">")
    channel_parts = channels.split(">")

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

    return " > ".join(new_parts)


def process_user_df(user_df):
    # Apply the function to create the 'removal_effect_ltv' column
    user_df["removal_effect_ltv"] = user_df.apply(calculate_removal_effect, axis=1)
    user_df = user_df.dropna(subset=["removal_effect_ltv"]).copy()

    # Split 'removal_effect_ltv' into a list of 'channel: ltv' strings
    user_df["channel_ltv_list"] = user_df["removal_effect_ltv"].str.split(" > ")

    # Explode the list to have one 'channel: ltv' per row
    df_exploded = user_df.explode("channel_ltv_list")

    # Split each 'channel_ltv' into 'channel' and 'ltv'
    df_exploded[["channel", "ltv"]] = df_exploded["channel_ltv_list"].str.split(
        ": ", n=1, expand=True
    )

    # Convert 'ltv' to numeric, handling any non-numeric values gracefully
    df_exploded["ltv"] = pd.to_numeric(df_exploded["ltv"], errors="coerce")

    # Group by 'channel' and 'run_date', then calculate the mean LTV
    average_ltv_per_channel = (
        df_exploded.groupby(["channel", "run_date"])["ltv"].mean().reset_index()
    )

    # Rename columns for clarity
    average_ltv_per_channel.rename(columns={"ltv": "average_ltv"}, inplace=True)

    return average_ltv_per_channel


# Applying the process to each DataFrame and storing the results
user_df_trial_30_avg = process_user_df(user_df_trial_30)
user_df_trial_60_avg = process_user_df(user_df_trial_60)
user_df_trial_90_avg = process_user_df(user_df_trial_90)

user_df_subscription_30_avg = process_user_df(user_df_subscription_30)
user_df_subscription_60_avg = process_user_df(user_df_subscription_60)
user_df_subscription_90_avg = process_user_df(user_df_subscription_90)

# user_df_registration_30_avg = process_user_df(user_df_registration_30)
# user_df_registration_60_avg = process_user_df(user_df_registration_60)
# user_df_registration_90_avg = process_user_df(user_df_registration_90)

# Adding the 'conversion_window' and 'conversion_type' columns to each DataFrame
user_df_trial_30_avg["conversion_window"] = 30
user_df_trial_30_avg["conversion_type"] = "Trial"

user_df_trial_60_avg["conversion_window"] = 60
user_df_trial_60_avg["conversion_type"] = "Trial"

user_df_trial_90_avg["conversion_window"] = 90
user_df_trial_90_avg["conversion_type"] = "Trial"

user_df_subscription_30_avg["conversion_window"] = 30
user_df_subscription_30_avg["conversion_type"] = "Subscription"

user_df_subscription_60_avg["conversion_window"] = 60
user_df_subscription_60_avg["conversion_type"] = "Subscription"

user_df_subscription_90_avg["conversion_window"] = 90
user_df_subscription_90_avg["conversion_type"] = "Subscription"

# user_df_registration_30_avg['conversion_window'] = 30
# user_df_registration_30_avg['conversion_type'] = 'Registration'

# user_df_registration_60_avg['conversion_window'] = 60
# user_df_registration_60_avg['conversion_type'] = 'Registration'

# user_df_registration_90_avg['conversion_window'] = 90
# user_df_registration_90_avg['conversion_type'] = 'Registration'

# Merging all DataFrames together
average_ltv_per_channel = pd.concat(
    [
        user_df_trial_30_avg,
        user_df_trial_60_avg,
        user_df_trial_90_avg,
        user_df_subscription_30_avg,
        user_df_subscription_60_avg,
        user_df_subscription_90_avg
        # user_df_registration_30_avg,
        # user_df_registration_60_avg,
        # user_df_registration_90_avg
    ],
    ignore_index=True,
)

normalized_removal_effects_all = normalized_removal_effects_all[
    normalized_removal_effects_all["removal_effect"] != 0
]
normalized_removal_effects_all = pd.merge(
    normalized_removal_effects_all,
    average_ltv_per_channel,
    left_on=("channel", "conversion_window", "conversion_type", "run_date"),
    right_on=("channel", "conversion_window", "conversion_type", "run_date"),
    how="left",
)


################################################ Upload data to BQ #########################################

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
    "ft-customer-analytics.crg_nniu.attribution_markov_transition_matrix_all": markov_transition_matrix_all,
    "ft-customer-analytics.crg_nniu.attribution_normalized_removal_effects_all": normalized_removal_effects_all,
    "ft-customer-analytics.crg_nniu.attribution_user_df_all": user_df_all,
    "ft-customer-analytics.crg_nniu.attribution_df_all": attribution_df_all,
    "ft-customer-analytics.crg_nniu.attribution_conversion_window_df": conversion_window_df,
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