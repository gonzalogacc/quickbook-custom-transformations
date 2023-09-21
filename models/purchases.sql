

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_scd`
select
    _airbyte_unique_key,
    CurrencyRef,
    ExchangeRate,
    TxnDate,
    airbyte_cursor,
    AccountRef,
    RemitToAddr,
    Line,
    SyncToken,
    Credit,
    sparse,
    TotalAmt,
    MetaData,
    domain,
    DocNumber,
    PurchaseEx,
    PaymentType,
    Id,
    PrintStatus,
    EntityRef,
    PrivateNote,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_purchases_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_scd`
-- purchases from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_purchases
where 1 = 1
and _airbyte_active_row = 1

  );
  