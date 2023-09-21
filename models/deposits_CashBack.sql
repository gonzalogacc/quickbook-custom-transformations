

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_CashBack`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__deposits_CashBack_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_scd`
select
    _airbyte_deposits_hashid,
    json_extract_scalar(CashBack, "$['Amount']") as Amount,
    
        json_extract(table_alias.CashBack, "$['AccountRef']")
     as AccountRef,
    json_extract_scalar(CashBack, "$['Memo']") as Memo,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_scd` as table_alias
-- CashBack at deposits/CashBack
where 1 = 1
and CashBack is not null

),  __dbt__cte__deposits_CashBack_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__deposits_CashBack_ab1
select
    _airbyte_deposits_hashid,
    cast(Amount as 
    float64
) as Amount,
    cast(AccountRef as 
    string
) as AccountRef,
    cast(Memo as 
    string
) as Memo,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__deposits_CashBack_ab1
-- CashBack at deposits/CashBack
where 1 = 1

),  __dbt__cte__deposits_CashBack_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__deposits_CashBack_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_deposits_hashid as 
    string
), ''), '-', coalesce(cast(Amount as 
    string
), ''), '-', coalesce(cast(AccountRef as 
    string
), ''), '-', coalesce(cast(Memo as 
    string
), '')) as 
    string
))) as _airbyte_CashBack_hashid,
    tmp.*
from __dbt__cte__deposits_CashBack_ab2 tmp
-- CashBack at deposits/CashBack
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__deposits_CashBack_ab3
select
    _airbyte_deposits_hashid,
    Amount,
    AccountRef,
    Memo,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_CashBack_hashid
from __dbt__cte__deposits_CashBack_ab3
-- CashBack at deposits/CashBack from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_scd`
where 1 = 1

  );
  