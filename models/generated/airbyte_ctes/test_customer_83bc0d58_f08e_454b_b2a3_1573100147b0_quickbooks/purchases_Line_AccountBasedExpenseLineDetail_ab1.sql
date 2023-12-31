{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('purchases_Line') }}
select
    _airbyte_Line_hashid,
    {{ json_extract('table_alias', 'AccountBasedExpenseLineDetail', ['TaxCodeRef'], ['TaxCodeRef']) }} as TaxCodeRef,
    {{ json_extract_scalar('AccountBasedExpenseLineDetail', ['BillableStatus'], ['BillableStatus']) }} as BillableStatus,
    {{ json_extract('table_alias', 'AccountBasedExpenseLineDetail', ['AccountRef'], ['AccountRef']) }} as AccountRef,
    {{ json_extract('table_alias', 'AccountBasedExpenseLineDetail', ['CustomerRef'], ['CustomerRef']) }} as CustomerRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('purchases_Line') }} as table_alias
-- AccountBasedExpenseLineDetail at purchases/Line/AccountBasedExpenseLineDetail
where 1 = 1
and AccountBasedExpenseLineDetail is not null

