{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('bill_payments_CheckPayment_ab1') }}
select
    _airbyte_bill_payments_hashid,
    cast(PrintStatus as {{ dbt_utils.type_string() }}) as PrintStatus,
    cast(BankAccountRef as {{ type_json() }}) as BankAccountRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('bill_payments_CheckPayment_ab1') }}
-- CheckPayment at bill_payments/CheckPayment
where 1 = 1
