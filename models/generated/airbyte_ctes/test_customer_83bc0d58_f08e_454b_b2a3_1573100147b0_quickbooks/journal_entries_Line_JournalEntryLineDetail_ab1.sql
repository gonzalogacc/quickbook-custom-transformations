{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('journal_entries_Line') }}
select
    _airbyte_Line_hashid,
    {{ json_extract_scalar('JournalEntryLineDetail', ['PostingType'], ['PostingType']) }} as PostingType,
    {{ json_extract('table_alias', 'JournalEntryLineDetail', ['AccountRef'], ['AccountRef']) }} as AccountRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('journal_entries_Line') }} as table_alias
-- JournalEntryLineDetail at journal_entries/Line/JournalEntryLineDetail
where 1 = 1
and JournalEntryLineDetail is not null

