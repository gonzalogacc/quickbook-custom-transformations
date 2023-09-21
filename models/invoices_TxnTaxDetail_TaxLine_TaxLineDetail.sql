

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_TxnTaxDetail_TaxLine_TaxLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_TxnTaxDetail_TaxLine`
select
    _airbyte_TaxLine_hashid,
    json_extract_scalar(TaxLineDetail, "$['PercentBased']") as PercentBased,
    
        json_extract(table_alias.TaxLineDetail, "$['TaxRateRef']")
     as TaxRateRef,
    json_extract_scalar(TaxLineDetail, "$['NetAmountTaxable']") as NetAmountTaxable,
    json_extract_scalar(TaxLineDetail, "$['TaxPercent']") as TaxPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_TxnTaxDetail_TaxLine` as table_alias
-- TaxLineDetail at invoices/TxnTaxDetail/TaxLine/TaxLineDetail
where 1 = 1
and TaxLineDetail is not null

),  __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab1
select
    _airbyte_TaxLine_hashid,
    cast(PercentBased as boolean) as PercentBased,
    cast(TaxRateRef as 
    string
) as TaxRateRef,
    cast(NetAmountTaxable as 
    float64
) as NetAmountTaxable,
    cast(TaxPercent as 
    float64
) as TaxPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab1
-- TaxLineDetail at invoices/TxnTaxDetail/TaxLine/TaxLineDetail
where 1 = 1

),  __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_TaxLine_hashid as 
    string
), ''), '-', coalesce(cast(PercentBased as 
    string
), ''), '-', coalesce(cast(TaxRateRef as 
    string
), ''), '-', coalesce(cast(NetAmountTaxable as 
    string
), ''), '-', coalesce(cast(TaxPercent as 
    string
), '')) as 
    string
))) as _airbyte_TaxLineDetail_hashid,
    tmp.*
from __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab2 tmp
-- TaxLineDetail at invoices/TxnTaxDetail/TaxLine/TaxLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab3
select
    _airbyte_TaxLine_hashid,
    PercentBased,
    TaxRateRef,
    NetAmountTaxable,
    TaxPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_TaxLineDetail_hashid
from __dbt__cte__invoices_TxnTaxDetail_TaxLine_TaxLineDetail_ab3
-- TaxLineDetail at invoices/TxnTaxDetail/TaxLine/TaxLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_TxnTaxDetail_TaxLine`
where 1 = 1

  );
  