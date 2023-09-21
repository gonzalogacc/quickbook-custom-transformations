

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_scd`
select
    _airbyte_unique_key,
    CurrencyRef,
    ExchangeRate,
    TaxRateRef,
    Adjustment,
    TxnDate,
    airbyte_cursor,
    Line,
    SyncToken,
    sparse,
    MetaData,
    domain,
    DocNumber,
    Id,
    PrivateNote,
    TxnTaxDetail,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_journal_entries_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_scd`
-- journal_entries from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_journal_entries
where 1 = 1
and _airbyte_active_row = 1

  );
  