{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('bills_Line_ab1') }}
select
    _airbyte_bills_hashid,
    cast(LineNum as {{ dbt_utils.type_bigint() }}) as LineNum,
    cast(ItemBasedExpenseLineDetail as {{ type_json() }}) as ItemBasedExpenseLineDetail,
    cast(Description as {{ dbt_utils.type_string() }}) as Description,
    cast(AccountBasedExpenseLineDetail as {{ type_json() }}) as AccountBasedExpenseLineDetail,
    cast(DetailType as {{ dbt_utils.type_string() }}) as DetailType,
    cast(Amount as {{ dbt_utils.type_float() }}) as Amount,
    cast(Id as {{ dbt_utils.type_string() }}) as Id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('bills_Line_ab1') }}
-- Line at bills/Line
where 1 = 1

