{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('sales_receipts_Line_DiscountLineDetail_ab1') }}
select
    _airbyte_Line_hashid,
    {{ cast_to_boolean('PercentBased') }} as PercentBased,
    cast(DiscountAccountRef as {{ type_json() }}) as DiscountAccountRef,
    cast(DiscountPercent as {{ dbt_utils.type_bigint() }}) as DiscountPercent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('sales_receipts_Line_DiscountLineDetail_ab1') }}
-- DiscountLineDetail at sales_receipts/Line/DiscountLineDetail
where 1 = 1

