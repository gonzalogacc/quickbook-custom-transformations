{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('deposits_Line') }}
{{ unnest_cte(ref('deposits_Line'), 'Line', 'LinkedTxn') }}
select
    _airbyte_Line_hashid,
    {{ json_extract_scalar(unnested_column_value('LinkedTxn'), ['TxnId'], ['TxnId']) }} as TxnId,
    {{ json_extract_scalar(unnested_column_value('LinkedTxn'), ['TxnType'], ['TxnType']) }} as TxnType,
    {{ json_extract_scalar(unnested_column_value('LinkedTxn'), ['TxnLineId'], ['TxnLineId']) }} as TxnLineId,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('deposits_Line') }} as table_alias
-- LinkedTxn at deposits/Line/LinkedTxn
{{ cross_join_unnest('Line', 'LinkedTxn') }}
where 1 = 1
and LinkedTxn is not null

