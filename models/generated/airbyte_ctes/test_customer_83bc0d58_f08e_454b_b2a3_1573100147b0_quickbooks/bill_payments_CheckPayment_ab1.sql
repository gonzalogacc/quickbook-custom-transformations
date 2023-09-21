{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('bill_payments') }}
select
    _airbyte_bill_payments_hashid,
    {{ json_extract_scalar('CheckPayment', ['PrintStatus'], ['PrintStatus']) }} as PrintStatus,
    {{ json_extract('table_alias', 'CheckPayment', ['BankAccountRef'], ['BankAccountRef']) }} as BankAccountRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('bill_payments') }} as table_alias
-- CheckPayment at bill_payments/CheckPayment
where 1 = 1
and CheckPayment is not null

