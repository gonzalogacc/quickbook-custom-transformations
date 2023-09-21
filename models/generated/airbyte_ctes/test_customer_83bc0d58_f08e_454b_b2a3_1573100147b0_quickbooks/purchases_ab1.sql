{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks', '_airbyte_raw_purchases') }}
select
    {{ json_extract('table_alias', '_airbyte_data', ['CurrencyRef'], ['CurrencyRef']) }} as CurrencyRef,
    {{ json_extract_scalar('_airbyte_data', ['ExchangeRate'], ['ExchangeRate']) }} as ExchangeRate,
    {{ json_extract_scalar('_airbyte_data', ['TxnDate'], ['TxnDate']) }} as TxnDate,
    {{ json_extract_scalar('_airbyte_data', ['airbyte_cursor'], ['airbyte_cursor']) }} as airbyte_cursor,
    {{ json_extract('table_alias', '_airbyte_data', ['AccountRef'], ['AccountRef']) }} as AccountRef,
    {{ json_extract('table_alias', '_airbyte_data', ['RemitToAddr'], ['RemitToAddr']) }} as RemitToAddr,
    {{ json_extract_array('_airbyte_data', ['Line'], ['Line']) }} as Line,
    {{ json_extract_scalar('_airbyte_data', ['SyncToken'], ['SyncToken']) }} as SyncToken,
    {{ json_extract_scalar('_airbyte_data', ['Credit'], ['Credit']) }} as Credit,
    {{ json_extract_scalar('_airbyte_data', ['sparse'], ['sparse']) }} as sparse,
    {{ json_extract_scalar('_airbyte_data', ['TotalAmt'], ['TotalAmt']) }} as TotalAmt,
    {{ json_extract('table_alias', '_airbyte_data', ['MetaData'], ['MetaData']) }} as MetaData,
    {{ json_extract_scalar('_airbyte_data', ['domain'], ['domain']) }} as domain,
    {{ json_extract_scalar('_airbyte_data', ['DocNumber'], ['DocNumber']) }} as DocNumber,
    {{ json_extract('table_alias', '_airbyte_data', ['PurchaseEx'], ['PurchaseEx']) }} as PurchaseEx,
    {{ json_extract_scalar('_airbyte_data', ['PaymentType'], ['PaymentType']) }} as PaymentType,
    {{ json_extract_scalar('_airbyte_data', ['Id'], ['Id']) }} as Id,
    {{ json_extract_scalar('_airbyte_data', ['PrintStatus'], ['PrintStatus']) }} as PrintStatus,
    {{ json_extract('table_alias', '_airbyte_data', ['EntityRef'], ['EntityRef']) }} as EntityRef,
    {{ json_extract_scalar('_airbyte_data', ['PrivateNote'], ['PrivateNote']) }} as PrivateNote,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks', '_airbyte_raw_purchases') }} as table_alias
-- purchases
where 1 = 1

