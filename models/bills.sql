

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_scd`
select
    _airbyte_unique_key,
    CurrencyRef,
    ExchangeRate,
    TxnDate,
    airbyte_cursor,
    DepartmentRef,
    Line,
    SyncToken,
    sparse,
    TotalAmt,
    MetaData,
    domain,
    DocNumber,
    APAccountRef,
    SalesTermRef,
    Id,
    DueDate,
    VendorRef,
    Balance,
    PrivateNote,
    LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_bills_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`bills_scd`
-- bills from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_bills
where 1 = 1
and _airbyte_active_row = 1

  );
  