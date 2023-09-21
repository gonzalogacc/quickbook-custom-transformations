

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_TxnTaxDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__journal_entries_TxnTaxDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_scd`
select
    _airbyte_journal_entries_hashid,
    json_extract_scalar(TxnTaxDetail, "$['TotalTax']") as TotalTax,
    
        json_extract(table_alias.TxnTaxDetail, "$['TxnTaxCodeRef']")
     as TxnTaxCodeRef,
    json_extract_array(TxnTaxDetail, "$['TaxLine']") as TaxLine,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_scd` as table_alias
-- TxnTaxDetail at journal_entries/TxnTaxDetail
where 1 = 1
and TxnTaxDetail is not null

),  __dbt__cte__journal_entries_TxnTaxDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__journal_entries_TxnTaxDetail_ab1
select
    _airbyte_journal_entries_hashid,
    cast(TotalTax as 
    float64
) as TotalTax,
    cast(TxnTaxCodeRef as 
    string
) as TxnTaxCodeRef,
    TaxLine,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__journal_entries_TxnTaxDetail_ab1
-- TxnTaxDetail at journal_entries/TxnTaxDetail
where 1 = 1

),  __dbt__cte__journal_entries_TxnTaxDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__journal_entries_TxnTaxDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_journal_entries_hashid as 
    string
), ''), '-', coalesce(cast(TotalTax as 
    string
), ''), '-', coalesce(cast(TxnTaxCodeRef as 
    string
), ''), '-', coalesce(cast(array_to_string(TaxLine, "|", "") as 
    string
), '')) as 
    string
))) as _airbyte_TxnTaxDetail_hashid,
    tmp.*
from __dbt__cte__journal_entries_TxnTaxDetail_ab2 tmp
-- TxnTaxDetail at journal_entries/TxnTaxDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__journal_entries_TxnTaxDetail_ab3
select
    _airbyte_journal_entries_hashid,
    TotalTax,
    TxnTaxCodeRef,
    TaxLine,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_TxnTaxDetail_hashid
from __dbt__cte__journal_entries_TxnTaxDetail_ab3
-- TxnTaxDetail at journal_entries/TxnTaxDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_scd`
where 1 = 1

  );
  