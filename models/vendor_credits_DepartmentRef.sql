

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`vendor_credits_DepartmentRef`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__vendor_credits_DepartmentRef_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`vendor_credits_scd`
select
    _airbyte_vendor_credits_hashid,
    json_extract_scalar(DepartmentRef, "$['name']") as name,
    json_extract_scalar(DepartmentRef, "$['value']") as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`vendor_credits_scd` as table_alias
-- DepartmentRef at vendor_credits/DepartmentRef
where 1 = 1
and DepartmentRef is not null

),  __dbt__cte__vendor_credits_DepartmentRef_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__vendor_credits_DepartmentRef_ab1
select
    _airbyte_vendor_credits_hashid,
    cast(name as 
    string
) as name,
    cast(value as 
    string
) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__vendor_credits_DepartmentRef_ab1
-- DepartmentRef at vendor_credits/DepartmentRef
where 1 = 1

),  __dbt__cte__vendor_credits_DepartmentRef_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__vendor_credits_DepartmentRef_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_vendor_credits_hashid as 
    string
), ''), '-', coalesce(cast(name as 
    string
), ''), '-', coalesce(cast(value as 
    string
), '')) as 
    string
))) as _airbyte_DepartmentRef_hashid,
    tmp.*
from __dbt__cte__vendor_credits_DepartmentRef_ab2 tmp
-- DepartmentRef at vendor_credits/DepartmentRef
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__vendor_credits_DepartmentRef_ab3
select
    _airbyte_vendor_credits_hashid,
    name,
    value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_DepartmentRef_hashid
from __dbt__cte__vendor_credits_DepartmentRef_ab3
-- DepartmentRef at vendor_credits/DepartmentRef from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`vendor_credits_scd`
where 1 = 1

  );
  