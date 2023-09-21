{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('journal_entries_TxnTaxDetail_TaxLine_ab1') }}
select
    _airbyte_TxnTaxDetail_hashid,
    cast(DetailType as {{ dbt_utils.type_string() }}) as DetailType,
    cast(TaxLineDetail as {{ type_json() }}) as TaxLineDetail,
    cast(Amount as {{ dbt_utils.type_float() }}) as Amount,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('journal_entries_TxnTaxDetail_TaxLine_ab1') }}
-- TaxLine at journal_entries/TxnTaxDetail/TaxLine
where 1 = 1

