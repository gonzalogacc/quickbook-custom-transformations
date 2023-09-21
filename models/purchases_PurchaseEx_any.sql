

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_PurchaseEx_any`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__purchases_PurchaseEx_any_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_PurchaseEx`

select
    _airbyte_PurchaseEx_hashid,
    json_extract_scalar(`any`, "$['nil']") as nil,
    json_extract_scalar(`any`, "$['typeSubstituted']") as typeSubstituted,
    json_extract_scalar(`any`, "$['globalScope']") as globalScope,
    json_extract_scalar(`any`, "$['scope']") as scope,
    json_extract_scalar(`any`, "$['name']") as name,
    json_extract_scalar(`any`, "$['declaredType']") as declaredType,
    
        json_extract(`any`, "$['value']")
     as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_PurchaseEx` as table_alias
-- any at purchases/PurchaseEx/any
cross join unnest(`any`) as `any`
where 1 = 1
and `any` is not null

),  __dbt__cte__purchases_PurchaseEx_any_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__purchases_PurchaseEx_any_ab1
select
    _airbyte_PurchaseEx_hashid,
    cast(nil as boolean) as nil,
    cast(typeSubstituted as boolean) as typeSubstituted,
    cast(globalScope as boolean) as globalScope,
    cast(scope as 
    string
) as scope,
    cast(name as 
    string
) as name,
    cast(declaredType as 
    string
) as declaredType,
    cast(value as 
    string
) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__purchases_PurchaseEx_any_ab1
-- any at purchases/PurchaseEx/any
where 1 = 1

),  __dbt__cte__purchases_PurchaseEx_any_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__purchases_PurchaseEx_any_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_PurchaseEx_hashid as 
    string
), ''), '-', coalesce(cast(nil as 
    string
), ''), '-', coalesce(cast(typeSubstituted as 
    string
), ''), '-', coalesce(cast(globalScope as 
    string
), ''), '-', coalesce(cast(scope as 
    string
), ''), '-', coalesce(cast(name as 
    string
), ''), '-', coalesce(cast(declaredType as 
    string
), ''), '-', coalesce(cast(value as 
    string
), '')) as 
    string
))) as _airbyte_any_hashid,
    tmp.*
from __dbt__cte__purchases_PurchaseEx_any_ab2 tmp
-- any at purchases/PurchaseEx/any
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__purchases_PurchaseEx_any_ab3
select
    _airbyte_PurchaseEx_hashid,
    nil,
    typeSubstituted,
    globalScope,
    scope,
    name,
    declaredType,
    value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_any_hashid
from __dbt__cte__purchases_PurchaseEx_any_ab3
-- any at purchases/PurchaseEx/any from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_PurchaseEx`
where 1 = 1

  );
  