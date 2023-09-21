

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts_scd`
select
    _airbyte_unique_key,
    CurrencyRef,
    airbyte_cursor,
    CurrentBalance,
    FullyQualifiedName,
    AccountType,
    Name,
    ParentRef,
    SyncToken,
    Active,
    sparse,
    MetaData,
    domain,
    Classification,
    CurrentBalanceWithSubAccounts,
    SubAccount,
    Id,
    AcctNum,
    AccountSubType,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_accounts_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`accounts_scd`
-- accounts from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_accounts
where 1 = 1
and _airbyte_active_row = 1

  );
  