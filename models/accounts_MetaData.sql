

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts_MetaData`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__accounts_MetaData_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts_scd`
select
    _airbyte_accounts_hashid,
    json_extract_scalar(MetaData, "$['CreateTime']") as CreateTime,
    json_extract_scalar(MetaData, "$['LastUpdatedTime']") as LastUpdatedTime,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts_scd` as table_alias
-- MetaData at accounts/MetaData
where 1 = 1
and MetaData is not null

),  __dbt__cte__accounts_MetaData_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__accounts_MetaData_ab1
select
    _airbyte_accounts_hashid,
    cast(nullif(CreateTime, '') as 
    timestamp
) as CreateTime,
    cast(nullif(LastUpdatedTime, '') as 
    timestamp
) as LastUpdatedTime,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__accounts_MetaData_ab1
-- MetaData at accounts/MetaData
where 1 = 1

),  __dbt__cte__accounts_MetaData_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__accounts_MetaData_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_accounts_hashid as 
    string
), ''), '-', coalesce(cast(CreateTime as 
    string
), ''), '-', coalesce(cast(LastUpdatedTime as 
    string
), '')) as 
    string
))) as _airbyte_MetaData_hashid,
    tmp.*
from __dbt__cte__accounts_MetaData_ab2 tmp
-- MetaData at accounts/MetaData
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__accounts_MetaData_ab3
select
    _airbyte_accounts_hashid,
    CreateTime,
    LastUpdatedTime,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_MetaData_hashid
from __dbt__cte__accounts_MetaData_ab3
-- MetaData at accounts/MetaData from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts_scd`
where 1 = 1

  );
  