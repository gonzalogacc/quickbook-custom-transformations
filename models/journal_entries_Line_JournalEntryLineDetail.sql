

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_Line_JournalEntryLineDetail`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_Line`
select
    _airbyte_Line_hashid,
    json_extract_scalar(JournalEntryLineDetail, "$['PostingType']") as PostingType,
    
        json_extract(table_alias.JournalEntryLineDetail, "$['AccountRef']")
     as AccountRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_Line` as table_alias
-- JournalEntryLineDetail at journal_entries/Line/JournalEntryLineDetail
where 1 = 1
and JournalEntryLineDetail is not null

),  __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab1
select
    _airbyte_Line_hashid,
    cast(PostingType as 
    string
) as PostingType,
    cast(AccountRef as 
    string
) as AccountRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab1
-- JournalEntryLineDetail at journal_entries/Line/JournalEntryLineDetail
where 1 = 1

),  __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(PostingType as 
    string
), ''), '-', coalesce(cast(AccountRef as 
    string
), '')) as 
    string
))) as _airbyte_JournalEntryLineDetail_hashid,
    tmp.*
from __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab2 tmp
-- JournalEntryLineDetail at journal_entries/Line/JournalEntryLineDetail
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab3
select
    _airbyte_Line_hashid,
    PostingType,
    AccountRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_JournalEntryLineDetail_hashid
from __dbt__cte__journal_entries_Line_JournalEntryLineDetail_ab3
-- JournalEntryLineDetail at journal_entries/Line/JournalEntryLineDetail from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`journal_entries_Line`
where 1 = 1

  );
  