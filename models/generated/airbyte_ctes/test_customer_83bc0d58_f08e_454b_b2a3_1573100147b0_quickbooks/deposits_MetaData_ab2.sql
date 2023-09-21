{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('deposits_MetaData_ab1') }}
select
    _airbyte_deposits_hashid,
    cast({{ empty_string_to_null('CreateTime') }} as {{ type_timestamp_with_timezone() }}) as CreateTime,
    cast({{ empty_string_to_null('LastUpdatedTime') }} as {{ type_timestamp_with_timezone() }}) as LastUpdatedTime,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('deposits_MetaData_ab1') }}
-- MetaData at deposits/MetaData
where 1 = 1

