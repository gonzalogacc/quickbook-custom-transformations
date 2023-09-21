

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CheckPayment_BankAccountRef`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CheckPayment`
select
    _airbyte_CheckPayment_hashid,
    json_extract_scalar(BankAccountRef, "$['name']") as name,
    json_extract_scalar(BankAccountRef, "$['value']") as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CheckPayment` as table_alias
-- BankAccountRef at bill_payments/CheckPayment/BankAccountRef
where 1 = 1
and BankAccountRef is not null

),  __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab1
select
    _airbyte_CheckPayment_hashid,
    cast(name as 
    string
) as name,
    cast(value as 
    string
) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab1
-- BankAccountRef at bill_payments/CheckPayment/BankAccountRef
where 1 = 1

),  __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_CheckPayment_hashid as 
    string
), ''), '-', coalesce(cast(name as 
    string
), ''), '-', coalesce(cast(value as 
    string
), '')) as 
    string
))) as _airbyte_BankAccountRef_hashid,
    tmp.*
from __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab2 tmp
-- BankAccountRef at bill_payments/CheckPayment/BankAccountRef
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab3
select
    _airbyte_CheckPayment_hashid,
    name,
    value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_BankAccountRef_hashid
from __dbt__cte__bill_payments_CheckPayment_BankAccountRef_ab3
-- BankAccountRef at bill_payments/CheckPayment/BankAccountRef from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CheckPayment`
where 1 = 1

  );
  