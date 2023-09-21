{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('bills_Line_AccountBasedExpenseLineDetail_ab1') }}
select
    _airbyte_Line_hashid,
    cast(TaxCodeRef as {{ type_json() }}) as TaxCodeRef,
    cast(BillableStatus as {{ dbt_utils.type_string() }}) as BillableStatus,
    cast(AccountRef as {{ type_json() }}) as AccountRef,
    cast(CustomerRef as {{ type_json() }}) as CustomerRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('bills_Line_AccountBasedExpenseLineDetail_ab1') }}
-- AccountBasedExpenseLineDetail at bills/Line/AccountBasedExpenseLineDetail
where 1 = 1

