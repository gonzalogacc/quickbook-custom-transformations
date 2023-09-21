{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks', '_airbyte_raw_credit_memos') }}
select
    {{ json_extract('table_alias', '_airbyte_data', ['CurrencyRef'], ['CurrencyRef']) }} as CurrencyRef,
    {{ json_extract_scalar('_airbyte_data', ['ExchangeRate'], ['ExchangeRate']) }} as ExchangeRate,
    {{ json_extract('table_alias', '_airbyte_data', ['ClassRef'], ['ClassRef']) }} as ClassRef,
    {{ json_extract_scalar('_airbyte_data', ['EmailStatus'], ['EmailStatus']) }} as EmailStatus,
    {{ json_extract('table_alias', '_airbyte_data', ['ShipAddr'], ['ShipAddr']) }} as ShipAddr,
    {{ json_extract_scalar('_airbyte_data', ['RemainingCredit'], ['RemainingCredit']) }} as RemainingCredit,
    {{ json_extract_scalar('_airbyte_data', ['HomeTotalAmt'], ['HomeTotalAmt']) }} as HomeTotalAmt,
    {{ json_extract('table_alias', '_airbyte_data', ['MetaData'], ['MetaData']) }} as MetaData,
    {{ json_extract_scalar('_airbyte_data', ['DocNumber'], ['DocNumber']) }} as DocNumber,
    {{ json_extract_scalar('_airbyte_data', ['PrintStatus'], ['PrintStatus']) }} as PrintStatus,
    {{ json_extract('table_alias', '_airbyte_data', ['BillEmail'], ['BillEmail']) }} as BillEmail,
    {{ json_extract('table_alias', '_airbyte_data', ['BillAddr'], ['BillAddr']) }} as BillAddr,
    {{ json_extract_scalar('_airbyte_data', ['TxnDate'], ['TxnDate']) }} as TxnDate,
    {{ json_extract_scalar('_airbyte_data', ['airbyte_cursor'], ['airbyte_cursor']) }} as airbyte_cursor,
    {{ json_extract('table_alias', '_airbyte_data', ['CustomerMemo'], ['CustomerMemo']) }} as CustomerMemo,
    {{ json_extract_array('_airbyte_data', ['Line'], ['Line']) }} as Line,
    {{ json_extract_scalar('_airbyte_data', ['SyncToken'], ['SyncToken']) }} as SyncToken,
    {{ json_extract_scalar('_airbyte_data', ['sparse'], ['sparse']) }} as sparse,
    {{ json_extract_scalar('_airbyte_data', ['TotalAmt'], ['TotalAmt']) }} as TotalAmt,
    {{ json_extract_scalar('_airbyte_data', ['domain'], ['domain']) }} as domain,
    {{ json_extract_array('_airbyte_data', ['CustomField'], ['CustomField']) }} as CustomField,
    {{ json_extract('table_alias', '_airbyte_data', ['SalesTermRef'], ['SalesTermRef']) }} as SalesTermRef,
    {{ json_extract_scalar('_airbyte_data', ['Id'], ['Id']) }} as Id,
    {{ json_extract('table_alias', '_airbyte_data', ['CustomerRef'], ['CustomerRef']) }} as CustomerRef,
    {{ json_extract_scalar('_airbyte_data', ['Balance'], ['Balance']) }} as Balance,
    {{ json_extract_scalar('_airbyte_data', ['ApplyTaxAfterDiscount'], ['ApplyTaxAfterDiscount']) }} as ApplyTaxAfterDiscount,
    {{ json_extract('table_alias', '_airbyte_data', ['TxnTaxDetail'], ['TxnTaxDetail']) }} as TxnTaxDetail,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks', '_airbyte_raw_credit_memos') }} as table_alias
-- credit_memos
where 1 = 1

