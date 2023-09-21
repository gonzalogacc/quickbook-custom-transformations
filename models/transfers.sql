

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`transfers`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`transfers_scd`
select
    _airbyte_unique_key,
    SyncToken,
    CurrencyRef,
    ExchangeRate,
    ToAccountRef,
    FromAccountRef,
    MetaData,
    Amount,
    domain,
    TxnDate,
    airbyte_cursor,
    Id,
    PrivateNote,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_transfers_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`transfers_scd`
-- transfers from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_transfers
where 1 = 1
and _airbyte_active_row = 1

  );
  