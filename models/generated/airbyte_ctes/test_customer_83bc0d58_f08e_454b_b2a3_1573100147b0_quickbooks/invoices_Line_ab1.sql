{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('invoices') }}
{{ unnest_cte(ref('invoices'), 'invoices', 'Line') }}
select
    _airbyte_invoices_hashid,
    {{ json_extract_scalar(unnested_column_value('Line'), ['LineNum'], ['LineNum']) }} as LineNum,
    {{ json_extract('', unnested_column_value('Line'), ['DiscountLineDetail'], ['DiscountLineDetail']) }} as DiscountLineDetail,
    {{ json_extract_scalar(unnested_column_value('Line'), ['Description'], ['Description']) }} as Description,
    {{ json_extract_scalar(unnested_column_value('Line'), ['DetailType'], ['DetailType']) }} as DetailType,
    {{ json_extract_scalar(unnested_column_value('Line'), ['Amount'], ['Amount']) }} as Amount,
    {{ json_extract('', unnested_column_value('Line'), ['SalesItemLineDetail'], ['SalesItemLineDetail']) }} as SalesItemLineDetail,
    {{ json_extract_scalar(unnested_column_value('Line'), ['Id'], ['Id']) }} as Id,
    {{ json_extract_array(unnested_column_value('Line'), ['LinkedTxn'], ['LinkedTxn']) }} as LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('invoices') }} as table_alias
-- Line at invoices/Line
{{ cross_join_unnest('invoices', 'Line') }}
where 1 = 1
and Line is not null

