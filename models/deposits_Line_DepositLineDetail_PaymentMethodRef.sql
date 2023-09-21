

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line_DepositLineDetail_PaymentMethodRef`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line_DepositLineDetail`
select
    _airbyte_DepositLineDetail_hashid,
    json_extract_scalar(PaymentMethodRef, "$['value']") as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line_DepositLineDetail` as table_alias
-- PaymentMethodRef at deposits/Line/DepositLineDetail/PaymentMethodRef
where 1 = 1
and PaymentMethodRef is not null

),  __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab1
select
    _airbyte_DepositLineDetail_hashid,
    cast(value as 
    string
) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab1
-- PaymentMethodRef at deposits/Line/DepositLineDetail/PaymentMethodRef
where 1 = 1

),  __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_DepositLineDetail_hashid as 
    string
), ''), '-', coalesce(cast(value as 
    string
), '')) as 
    string
))) as _airbyte_PaymentMethodRef_hashid,
    tmp.*
from __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab2 tmp
-- PaymentMethodRef at deposits/Line/DepositLineDetail/PaymentMethodRef
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab3
select
    _airbyte_DepositLineDetail_hashid,
    value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_PaymentMethodRef_hashid
from __dbt__cte__deposits_Line_DepositLineDetail_PaymentMethodRef_ab3
-- PaymentMethodRef at deposits/Line/DepositLineDetail/PaymentMethodRef from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line_DepositLineDetail`
where 1 = 1

  );
  