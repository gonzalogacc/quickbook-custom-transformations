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
    {{ json_extract_scalar('ItemBasedExpenseLineDetail', ['UnitPrice'], ['UnitPrice']) }} as UnitPrice,
    {{ json_extract('table_alias', 'ItemBasedExpenseLineDetail', ['TaxCodeRef'], ['TaxCodeRef']) }} as TaxCodeRef,
    {{ json_extract_scalar('ItemBasedExpenseLineDetail', ['BillableStatus'], ['BillableStatus']) }} as BillableStatus,
    {{ json_extract_scalar('ItemBasedExpenseLineDetail', ['Qty'], ['Qty']) }} as Qty,
    {{ json_extract('table_alias', 'ItemBasedExpenseLineDetail', ['ItemRef'], ['ItemRef']) }} as ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('purchases_Line') }} as table_alias
-- ItemBasedExpenseLineDetail at purchases/Line/ItemBasedExpenseLineDetail
where 1 = 1
and ItemBasedExpenseLineDetail is not null

