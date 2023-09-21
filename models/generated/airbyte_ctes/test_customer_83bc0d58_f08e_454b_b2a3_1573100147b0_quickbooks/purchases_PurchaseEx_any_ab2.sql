{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('purchases_PurchaseEx_any_ab1') }}
select
    _airbyte_PurchaseEx_hashid,
    {{ cast_to_boolean('nil') }} as nil,
    {{ cast_to_boolean('typeSubstituted') }} as typeSubstituted,
    {{ cast_to_boolean('globalScope') }} as globalScope,
    cast(scope as {{ dbt_utils.type_string() }}) as scope,
    cast(name as {{ dbt_utils.type_string() }}) as name,
    cast(declaredType as {{ dbt_utils.type_string() }}) as declaredType,
    cast(value as {{ type_json() }}) as value,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('purchases_PurchaseEx_any_ab1') }}
-- any at purchases/PurchaseEx/any
where 1 = 1

