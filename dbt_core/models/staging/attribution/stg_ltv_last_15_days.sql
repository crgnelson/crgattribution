{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('bi_layer_integration', 'known_user_daily_status') }}

),

filtered as (

    select
        user_guid,
        ltv_acquisition_capped_12m,
        product_order_timestamp,
        product_arrangement_id
    from source
    where date(product_order_timestamp) >= date_sub(current_date(), interval 15 day)
      and ltv_acquisition_capped_12m is not null
      and product_order_timestamp is not null
      and product_arrangement_id is not null

)

select * from filtered
