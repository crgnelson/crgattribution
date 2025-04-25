-- models/intermediate/int_conversion_windows_30.sql
{{ config(materialized='table') }}

{{ generate_conversion_window_model(31) }}
