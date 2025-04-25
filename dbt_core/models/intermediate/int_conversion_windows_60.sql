-- models/intermediate/int_conversion_windows_60.sql
{{ config(materialized='table') }}

{{ generate_conversion_window_model(61) }}
