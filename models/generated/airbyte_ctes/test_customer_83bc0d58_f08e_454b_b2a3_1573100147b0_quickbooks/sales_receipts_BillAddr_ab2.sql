{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('sales_receipts_BillAddr_ab1') }}
select
    _airbyte_sales_receipts_hashid,
    cast(Line4 as {{ dbt_utils.type_string() }}) as Line4,
    cast(Long as {{ dbt_utils.type_string() }}) as Long,
    cast(Id as {{ dbt_utils.type_string() }}) as Id,
    cast(Line1 as {{ dbt_utils.type_string() }}) as Line1,
    cast(Lat as {{ dbt_utils.type_string() }}) as Lat,
    cast(Line2 as {{ dbt_utils.type_string() }}) as Line2,
    cast(Line3 as {{ dbt_utils.type_string() }}) as Line3,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('sales_receipts_BillAddr_ab1') }}
-- BillAddr at sales_receipts/BillAddr
where 1 = 1

