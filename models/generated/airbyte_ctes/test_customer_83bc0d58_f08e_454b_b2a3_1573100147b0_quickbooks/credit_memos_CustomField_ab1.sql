{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('credit_memos') }}
{{ unnest_cte(ref('credit_memos'), 'credit_memos', 'CustomField') }}
select
    _airbyte_credit_memos_hashid,
    {{ json_extract_scalar(unnested_column_value('CustomField'), ['Type'], ['Type']) }} as Type,
    {{ json_extract_scalar(unnested_column_value('CustomField'), ['DefinitionId'], ['DefinitionId']) }} as DefinitionId,
    {{ json_extract_scalar(unnested_column_value('CustomField'), ['Name'], ['Name']) }} as Name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('credit_memos') }} as table_alias
-- CustomField at credit_memos/CustomField
{{ cross_join_unnest('credit_memos', 'CustomField') }}
where 1 = 1
and CustomField is not null

