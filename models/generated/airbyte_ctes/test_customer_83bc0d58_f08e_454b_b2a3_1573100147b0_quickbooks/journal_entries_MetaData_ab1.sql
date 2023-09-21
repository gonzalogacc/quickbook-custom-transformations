{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('journal_entries') }}
select
    _airbyte_journal_entries_hashid,
    {{ json_extract_scalar('MetaData', ['CreateTime'], ['CreateTime']) }} as CreateTime,
    {{ json_extract_scalar('MetaData', ['LastUpdatedTime'], ['LastUpdatedTime']) }} as LastUpdatedTime,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('journal_entries') }} as table_alias
-- MetaData at journal_entries/MetaData
where 1 = 1
and MetaData is not null

