

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts_Line_DiscountLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts_Line`
select
    _airbyte_Line_hashid,
    json_extract_scalar(DiscountLineDetail, "$['PercentBased']") as PercentBased,
    
        json_extract(table_alias.DiscountLineDetail, "$['DiscountAccountRef']")
     as DiscountAccountRef,
    json_extract_scalar(DiscountLineDetail, "$['DiscountPercent']") as DiscountPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts_Line` as table_alias
-- DiscountLineDetail at sales_receipts/Line/DiscountLineDetail
where 1 = 1
and DiscountLineDetail is not null

),  __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab1
select
    _airbyte_Line_hashid,
    cast(PercentBased as boolean) as PercentBased,
    cast(DiscountAccountRef as 
    string
) as DiscountAccountRef,
    cast(DiscountPercent as 
    int64
) as DiscountPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab1
-- DiscountLineDetail at sales_receipts/Line/DiscountLineDetail
where 1 = 1

),  __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(PercentBased as 
    string
), ''), '-', coalesce(cast(DiscountAccountRef as 
    string
), ''), '-', coalesce(cast(DiscountPercent as 
    string
), '')) as 
    string
))) as _airbyte_DiscountLineDetail_hashid,
    tmp.*
from __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab2 tmp
-- DiscountLineDetail at sales_receipts/Line/DiscountLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab3
select
    _airbyte_Line_hashid,
    PercentBased,
    DiscountAccountRef,
    DiscountPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_DiscountLineDetail_hashid
from __dbt__cte__sales_receipts_Line_DiscountLineDetail_ab3
-- DiscountLineDetail at sales_receipts/Line/DiscountLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts_Line`
where 1 = 1

  );
  