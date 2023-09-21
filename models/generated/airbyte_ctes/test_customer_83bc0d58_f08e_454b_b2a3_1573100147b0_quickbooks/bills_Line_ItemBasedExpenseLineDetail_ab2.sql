{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('bills_Line_ItemBasedExpenseLineDetail_ab1') }}
select
    _airbyte_Line_hashid,
    cast(UnitPrice as {{ dbt_utils.type_float() }}) as UnitPrice,
    cast(TaxCodeRef as {{ type_json() }}) as TaxCodeRef,
    cast(BillableStatus as {{ dbt_utils.type_string() }}) as BillableStatus,
    cast(Qty as {{ dbt_utils.type_bigint() }}) as Qty,
    cast(ItemRef as {{ type_json() }}) as ItemRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('bills_Line_ItemBasedExpenseLineDetail_ab1') }}
-- ItemBasedExpenseLineDetail at bills/Line/ItemBasedExpenseLineDetail
where 1 = 1
