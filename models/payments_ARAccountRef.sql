

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_ARAccountRef`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__payments_ARAccountRef_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_scd`
select
    _airbyte_payments_hashid,
    json_extract_scalar(ARAccountRef, "$['name']") as name,
    json_extract_scalar(ARAccountRef, "$['value']") as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_scd` as table_alias
-- ARAccountRef at payments/ARAccountRef
where 1 = 1
and ARAccountRef is not null

),  __dbt__cte__payments_ARAccountRef_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__payments_ARAccountRef_ab1
select
    _airbyte_payments_hashid,
    cast(name as 
    string
) as name,
    cast(value as 
    string
) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__payments_ARAccountRef_ab1
-- ARAccountRef at payments/ARAccountRef
where 1 = 1

),  __dbt__cte__payments_ARAccountRef_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__payments_ARAccountRef_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_payments_hashid as 
    string
), ''), '-', coalesce(cast(name as 
    string
), ''), '-', coalesce(cast(value as 
    string
), '')) as 
    string
))) as _airbyte_ARAccountRef_hashid,
    tmp.*
from __dbt__cte__payments_ARAccountRef_ab2 tmp
-- ARAccountRef at payments/ARAccountRef
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__payments_ARAccountRef_ab3
select
    _airbyte_payments_hashid,
    name,
    value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_ARAccountRef_hashid
from __dbt__cte__payments_ARAccountRef_ab3
-- ARAccountRef at payments/ARAccountRef from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_scd`
where 1 = 1

  );
  