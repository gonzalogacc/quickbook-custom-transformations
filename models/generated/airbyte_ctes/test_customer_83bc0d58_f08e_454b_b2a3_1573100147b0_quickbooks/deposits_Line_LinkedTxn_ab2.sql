{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('deposits_Line_LinkedTxn_ab1') }}
select
    _airbyte_Line_hashid,
    cast(TxnId as {{ dbt_utils.type_string() }}) as TxnId,
    cast(TxnType as {{ dbt_utils.type_string() }}) as TxnType,
    cast(TxnLineId as {{ dbt_utils.type_string() }}) as TxnLineId,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('deposits_Line_LinkedTxn_ab1') }}
-- LinkedTxn at deposits/Line/LinkedTxn
where 1 = 1

