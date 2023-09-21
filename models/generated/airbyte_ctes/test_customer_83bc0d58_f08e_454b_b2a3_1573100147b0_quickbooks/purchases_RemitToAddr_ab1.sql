{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('purchases') }}
select
    _airbyte_purchases_hashid,
    {{ json_extract_scalar('RemitToAddr', ['CountrySubDivisionCode'], ['CountrySubDivisionCode']) }} as CountrySubDivisionCode,
    {{ json_extract_scalar('RemitToAddr', ['Long'], ['Long']) }} as Long,
    {{ json_extract_scalar('RemitToAddr', ['PostalCode'], ['PostalCode']) }} as PostalCode,
    {{ json_extract_scalar('RemitToAddr', ['Id'], ['Id']) }} as Id,
    {{ json_extract_scalar('RemitToAddr', ['City'], ['City']) }} as City,
    {{ json_extract_scalar('RemitToAddr', ['Line1'], ['Line1']) }} as Line1,
    {{ json_extract_scalar('RemitToAddr', ['Lat'], ['Lat']) }} as Lat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('purchases') }} as table_alias
-- RemitToAddr at purchases/RemitToAddr
where 1 = 1
and RemitToAddr is not null

