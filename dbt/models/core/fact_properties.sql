{{ config(materialized='table') }}

with sell_data as (
    select *
    from {{ ref('stg_sell_properti_data') }}
), 

rent_data as (
    select *
    from {{ ref('stg_rent_properti_data') }}
), 

properti_unioned as (
    select * from sell_data
    union all
    select * from rent_data
)
select 
    properti_unioned.propertiid,
    properti_unioned.id,
    properti_unioned.operation,
    properti_unioned.property_type,
    properti_unioned.place_name,
    properti_unioned.state_name,
    properti_unioned.created_on,
    properti_unioned.surface_total,
    properti_unioned.surface_covered,
    properti_unioned.floor,
    properti_unioned.rooms,
    properti_unioned.price,
    properti_unioned.currency,
    properti_unioned.price_approx_in_brl,
    properti_unioned.price_approx_in_usd,
    properti_unioned.price_per_m2_in_usd,
    properti_unioned.price_per_m2_in_brl
from properti_unioned