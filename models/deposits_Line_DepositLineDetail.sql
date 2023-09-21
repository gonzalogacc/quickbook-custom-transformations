

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line_DepositLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__deposits_Line_DepositLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line`
select
    _airbyte_Line_hashid,
    
        json_extract(table_alias.DepositLineDetail, "$['PaymentMethodRef']")
     as PaymentMethodRef,
    
        json_extract(table_alias.DepositLineDetail, "$['AccountRef']")
     as AccountRef,
    json_extract_scalar(DepositLineDetail, "$['CheckNum']") as CheckNum,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line` as table_alias
-- DepositLineDetail at deposits/Line/DepositLineDetail
where 1 = 1
and DepositLineDetail is not null

),  __dbt__cte__deposits_Line_DepositLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__deposits_Line_DepositLineDetail_ab1
select
    _airbyte_Line_hashid,
    cast(PaymentMethodRef as 
    string
) as PaymentMethodRef,
    cast(AccountRef as 
    string
) as AccountRef,
    cast(CheckNum as 
    string
) as CheckNum,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__deposits_Line_DepositLineDetail_ab1
-- DepositLineDetail at deposits/Line/DepositLineDetail
where 1 = 1

),  __dbt__cte__deposits_Line_DepositLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__deposits_Line_DepositLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(PaymentMethodRef as 
    string
), ''), '-', coalesce(cast(AccountRef as 
    string
), ''), '-', coalesce(cast(CheckNum as 
    string
), '')) as 
    string
))) as _airbyte_DepositLineDetail_hashid,
    tmp.*
from __dbt__cte__deposits_Line_DepositLineDetail_ab2 tmp
-- DepositLineDetail at deposits/Line/DepositLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__deposits_Line_DepositLineDetail_ab3
select
    _airbyte_Line_hashid,
    PaymentMethodRef,
    AccountRef,
    CheckNum,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_DepositLineDetail_hashid
from __dbt__cte__deposits_Line_DepositLineDetail_ab3
-- DepositLineDetail at deposits/Line/DepositLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line`
where 1 = 1

  );
  