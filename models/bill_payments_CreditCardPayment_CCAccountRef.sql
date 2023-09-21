

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CreditCardPayment_CCAccountRef`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CreditCardPayment`
select
    _airbyte_CreditCardPayment_hashid,
    json_extract_scalar(CCAccountRef, "$['name']") as name,
    json_extract_scalar(CCAccountRef, "$['value']") as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CreditCardPayment` as table_alias
-- CCAccountRef at bill_payments/CreditCardPayment/CCAccountRef
where 1 = 1
and CCAccountRef is not null

),  __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab1
select
    _airbyte_CreditCardPayment_hashid,
    cast(name as 
    string
) as name,
    cast(value as 
    string
) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab1
-- CCAccountRef at bill_payments/CreditCardPayment/CCAccountRef
where 1 = 1

),  __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_CreditCardPayment_hashid as 
    string
), ''), '-', coalesce(cast(name as 
    string
), ''), '-', coalesce(cast(value as 
    string
), '')) as 
    string
))) as _airbyte_CCAccountRef_hashid,
    tmp.*
from __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab2 tmp
-- CCAccountRef at bill_payments/CreditCardPayment/CCAccountRef
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab3
select
    _airbyte_CreditCardPayment_hashid,
    name,
    value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_CCAccountRef_hashid
from __dbt__cte__bill_payments_CreditCardPayment_CCAccountRef_ab3
-- CCAccountRef at bill_payments/CreditCardPayment/CCAccountRef from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bill_payments_CreditCardPayment`
where 1 = 1

  );
  