

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_TxnTaxDetail_TaxLine`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_TxnTaxDetail`

select
    _airbyte_TxnTaxDetail_hashid,
    json_extract_scalar(TaxLine, "$['DetailType']") as DetailType,
    
        json_extract(TaxLine, "$['TaxLineDetail']")
     as TaxLineDetail,
    json_extract_scalar(TaxLine, "$['Amount']") as Amount,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_TxnTaxDetail` as table_alias
-- TaxLine at journal_entries/TxnTaxDetail/TaxLine
cross join unnest(TaxLine) as TaxLine
where 1 = 1
and TaxLine is not null

),  __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab1
select
    _airbyte_TxnTaxDetail_hashid,
    cast(DetailType as 
    string
) as DetailType,
    cast(TaxLineDetail as 
    string
) as TaxLineDetail,
    cast(Amount as 
    float64
) as Amount,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab1
-- TaxLine at journal_entries/TxnTaxDetail/TaxLine
where 1 = 1

),  __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_TxnTaxDetail_hashid as 
    string
), ''), '-', coalesce(cast(DetailType as 
    string
), ''), '-', coalesce(cast(TaxLineDetail as 
    string
), ''), '-', coalesce(cast(Amount as 
    string
), '')) as 
    string
))) as _airbyte_TaxLine_hashid,
    tmp.*
from __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab2 tmp
-- TaxLine at journal_entries/TxnTaxDetail/TaxLine
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab3
select
    _airbyte_TxnTaxDetail_hashid,
    DetailType,
    TaxLineDetail,
    Amount,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_TaxLine_hashid
from __dbt__cte__journal_entries_TxnTaxDetail_TaxLine_ab3
-- TaxLine at journal_entries/TxnTaxDetail/TaxLine from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_TxnTaxDetail`
where 1 = 1

  );
  