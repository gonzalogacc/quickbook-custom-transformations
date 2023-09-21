{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('invoices_Line_DiscountLineDetail') }}
select
    _airbyte_DiscountLineDetail_hashid,
    {{ json_extract_scalar('DiscountAccountRef', ['name'], ['name']) }} as name,
    {{ json_extract_scalar('DiscountAccountRef', ['value'], ['value']) }} as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('invoices_Line_DiscountLineDetail') }} as table_alias
-- DiscountAccountRef at invoices/Line/DiscountLineDetail/DiscountAccountRef
where 1 = 1
and DiscountAccountRef is not null

