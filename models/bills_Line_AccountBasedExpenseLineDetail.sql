

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line_AccountBasedExpenseLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line`
select
    _airbyte_Line_hashid,
    
        json_extract(table_alias.AccountBasedExpenseLineDetail, "$['TaxCodeRef']")
     as TaxCodeRef,
    json_extract_scalar(AccountBasedExpenseLineDetail, "$['BillableStatus']") as BillableStatus,
    
        json_extract(table_alias.AccountBasedExpenseLineDetail, "$['AccountRef']")
     as AccountRef,
    
        json_extract(table_alias.AccountBasedExpenseLineDetail, "$['CustomerRef']")
     as CustomerRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line` as table_alias
-- AccountBasedExpenseLineDetail at bills/Line/AccountBasedExpenseLineDetail
where 1 = 1
and AccountBasedExpenseLineDetail is not null

),  __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab1
select
    _airbyte_Line_hashid,
    cast(TaxCodeRef as 
    string
) as TaxCodeRef,
    cast(BillableStatus as 
    string
) as BillableStatus,
    cast(AccountRef as 
    string
) as AccountRef,
    cast(CustomerRef as 
    string
) as CustomerRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab1
-- AccountBasedExpenseLineDetail at bills/Line/AccountBasedExpenseLineDetail
where 1 = 1

),  __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(TaxCodeRef as 
    string
), ''), '-', coalesce(cast(BillableStatus as 
    string
), ''), '-', coalesce(cast(AccountRef as 
    string
), ''), '-', coalesce(cast(CustomerRef as 
    string
), '')) as 
    string
))) as _airbyte_AccountBasedExpenseLineDetail_hashid,
    tmp.*
from __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab2 tmp
-- AccountBasedExpenseLineDetail at bills/Line/AccountBasedExpenseLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab3
select
    _airbyte_Line_hashid,
    TaxCodeRef,
    BillableStatus,
    AccountRef,
    CustomerRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_AccountBasedExpenseLineDetail_hashid
from __dbt__cte__bills_Line_AccountBasedExpenseLineDetail_ab3
-- AccountBasedExpenseLineDetail at bills/Line/AccountBasedExpenseLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line`
where 1 = 1

  );
  