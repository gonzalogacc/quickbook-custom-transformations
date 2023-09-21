{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('bills_Line_AccountBasedExpenseLineDetail') }}
select
    _airbyte_AccountBasedExpenseLineDetail_hashid,
    {{ json_extract_scalar('AccountRef', ['name'], ['name']) }} as name,
    {{ json_extract_scalar('AccountRef', ['value'], ['value']) }} as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('bills_Line_AccountBasedExpenseLineDetail') }} as table_alias
-- AccountRef at bills/Line/AccountBasedExpenseLineDetail/AccountRef
where 1 = 1
and AccountRef is not null

