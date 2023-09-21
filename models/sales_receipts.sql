

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts_scd`
select
    _airbyte_unique_key,
    CurrencyRef,
    ExchangeRate,
    EmailStatus,
    ShipAddr,
    HomeTotalAmt,
    MetaData,
    PaymentMethodRef,
    DocNumber,
    PrintStatus,
    BillEmail,
    LinkedTxn,
    BillAddr,
    PaymentRefNum,
    TxnDate,
    airbyte_cursor,
    CustomerMemo,
    Line,
    SyncToken,
    DepositToAccountRef,
    sparse,
    TotalAmt,
    domain,
    CustomField,
    Id,
    CustomerRef,
    Balance,
    ApplyTaxAfterDiscount,
    TxnTaxDetail,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_sales_receipts_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`sales_receipts_scd`
-- sales_receipts from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_sales_receipts
where 1 = 1
and _airbyte_active_row = 1

  );
  