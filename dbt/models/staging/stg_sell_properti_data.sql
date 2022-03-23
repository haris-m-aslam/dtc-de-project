{{ config(materialized='view') }}

with propertidata as 
(
  select *,
    row_number() over(partition by id, created_on) as rn
  from {{ source('staging','sell_data_partitioned') }}
  -- where id is not null 
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['id', 'created_on']) }} as propertiid,
    cast(id as string) as id,    
    cast(title as string) as title,
    cast(operation as string) as operation,
    cast(property_type as string) as property_type,
    cast(place_name as string) as place_name,
    cast(state_name as string) as state_name,
    
    -- timestamps
    cast(created_on as date) as created_on,
    
    -- properti info
    cast(surface_total_in_m2 as numeric) as surface_total,
    cast(surface_covered_in_m2 as numeric) as surface_covered,
    cast(floor as integer) as floor,
    cast(rooms as integer) as rooms,
    
    -- payment info
    cast(price as numeric) as price,
    cast(currency as string) as currency,
    cast(price_aprox_local_currency as numeric) as price_approx_in_brl,
    cast(price_aprox_usd as numeric) as price_approx_in_usd,
    cast(price_usd_per_m2 as numeric) as price_per_m2_in_usd,
    cast(price_per_m2 as numeric) as price_per_m2_in_brl
from propertidata
-- where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
