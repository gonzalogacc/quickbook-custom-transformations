{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('refund_receipts_TxnTaxDetail_ab1') }}
select
    _airbyte_refund_receipts_hashid,
    cast(TotalTax as {{ dbt_utils.type_bigint() }}) as TotalTax,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('refund_receipts_TxnTaxDetail_ab1') }}
-- TxnTaxDetail at refund_receipts/TxnTaxDetail
where 1 = 1

