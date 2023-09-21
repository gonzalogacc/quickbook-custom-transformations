{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('sales_receipts_ShipAddr_ab1') }}
select
    _airbyte_sales_receipts_hashid,
    cast(CountrySubDivisionCode as {{ dbt_utils.type_string() }}) as CountrySubDivisionCode,
    cast(Long as {{ dbt_utils.type_string() }}) as Long,
    cast(Country as {{ dbt_utils.type_string() }}) as Country,
    cast(PostalCode as {{ dbt_utils.type_string() }}) as PostalCode,
    cast(Id as {{ dbt_utils.type_string() }}) as Id,
    cast(City as {{ dbt_utils.type_string() }}) as City,
    cast(Line1 as {{ dbt_utils.type_string() }}) as Line1,
    cast(Lat as {{ dbt_utils.type_string() }}) as Lat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('sales_receipts_ShipAddr_ab1') }}
-- ShipAddr at sales_receipts/ShipAddr
where 1 = 1
