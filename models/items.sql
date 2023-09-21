

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`items`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_unique_key, _airbyte_emitted_at
  OPTIONS()
  as (
    
-- Final base SQL model
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`items_scd`
select
    _airbyte_unique_key,
    Description,
    QtyOnHand,
    airbyte_cursor,
    FullyQualifiedName,
    PurchaseDesc,
    TrackQtyOnHand,
    AssetAccountRef,
    IncomeAccountRef,
    Name,
    SyncToken,
    Type,
    Active,
    UnitPrice,
    ExpenseAccountRef,
    sparse,
    MetaData,
    PurchaseCost,
    domain,
    InvStartDate,
    Id,
    Taxable,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_items_hashid
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`items_scd`
-- items from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks._airbyte_raw_items
where 1 = 1
and _airbyte_active_row = 1

  );
  