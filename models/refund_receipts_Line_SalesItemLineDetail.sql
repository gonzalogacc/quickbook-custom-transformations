

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`refund_receipts_Line_SalesItemLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`refund_receipts_Line`
select
    _airbyte_Line_hashid,
    json_extract_scalar(SalesItemLineDetail, "$['UnitPrice']") as UnitPrice,
    
        json_extract(table_alias.SalesItemLineDetail, "$['TaxCodeRef']")
     as TaxCodeRef,
    json_extract_scalar(SalesItemLineDetail, "$['Qty']") as Qty,
    
        json_extract(table_alias.SalesItemLineDetail, "$['ItemRef']")
     as ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`refund_receipts_Line` as table_alias
-- SalesItemLineDetail at refund_receipts/Line/SalesItemLineDetail
where 1 = 1
and SalesItemLineDetail is not null

),  __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab1
select
    _airbyte_Line_hashid,
    cast(UnitPrice as 
    int64
) as UnitPrice,
    cast(TaxCodeRef as 
    string
) as TaxCodeRef,
    cast(Qty as 
    float64
) as Qty,
    cast(ItemRef as 
    string
) as ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab1
-- SalesItemLineDetail at refund_receipts/Line/SalesItemLineDetail
where 1 = 1

),  __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(UnitPrice as 
    string
), ''), '-', coalesce(cast(TaxCodeRef as 
    string
), ''), '-', coalesce(cast(Qty as 
    string
), ''), '-', coalesce(cast(ItemRef as 
    string
), '')) as 
    string
))) as _airbyte_SalesItemLineDetail_hashid,
    tmp.*
from __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab2 tmp
-- SalesItemLineDetail at refund_receipts/Line/SalesItemLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab3
select
    _airbyte_Line_hashid,
    UnitPrice,
    TaxCodeRef,
    Qty,
    ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_SalesItemLineDetail_hashid
from __dbt__cte__refund_receipts_Line_SalesItemLineDetail_ab3
-- SalesItemLineDetail at refund_receipts/Line/SalesItemLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`refund_receipts_Line`
where 1 = 1

  );
  