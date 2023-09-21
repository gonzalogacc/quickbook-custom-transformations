

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line_ItemBasedExpenseLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line`
select
    _airbyte_Line_hashid,
    json_extract_scalar(ItemBasedExpenseLineDetail, "$['UnitPrice']") as UnitPrice,
    
        json_extract(table_alias.ItemBasedExpenseLineDetail, "$['TaxCodeRef']")
     as TaxCodeRef,
    json_extract_scalar(ItemBasedExpenseLineDetail, "$['BillableStatus']") as BillableStatus,
    json_extract_scalar(ItemBasedExpenseLineDetail, "$['Qty']") as Qty,
    
        json_extract(table_alias.ItemBasedExpenseLineDetail, "$['ItemRef']")
     as ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line` as table_alias
-- ItemBasedExpenseLineDetail at bills/Line/ItemBasedExpenseLineDetail
where 1 = 1
and ItemBasedExpenseLineDetail is not null

),  __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab1
select
    _airbyte_Line_hashid,
    cast(UnitPrice as 
    int64
) as UnitPrice,
    cast(TaxCodeRef as 
    string
) as TaxCodeRef,
    cast(BillableStatus as 
    string
) as BillableStatus,
    cast(Qty as 
    int64
) as Qty,
    cast(ItemRef as 
    string
) as ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab1
-- ItemBasedExpenseLineDetail at bills/Line/ItemBasedExpenseLineDetail
where 1 = 1

),  __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(UnitPrice as 
    string
), ''), '-', coalesce(cast(TaxCodeRef as 
    string
), ''), '-', coalesce(cast(BillableStatus as 
    string
), ''), '-', coalesce(cast(Qty as 
    string
), ''), '-', coalesce(cast(ItemRef as 
    string
), '')) as 
    string
))) as _airbyte_ItemBasedExpenseLineDetail_hashid,
    tmp.*
from __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab2 tmp
-- ItemBasedExpenseLineDetail at bills/Line/ItemBasedExpenseLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab3
select
    _airbyte_Line_hashid,
    UnitPrice,
    TaxCodeRef,
    BillableStatus,
    Qty,
    ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_ItemBasedExpenseLineDetail_hashid
from __dbt__cte__bills_Line_ItemBasedExpenseLineDetail_ab3
-- ItemBasedExpenseLineDetail at bills/Line/ItemBasedExpenseLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_Line`
where 1 = 1

  );
  