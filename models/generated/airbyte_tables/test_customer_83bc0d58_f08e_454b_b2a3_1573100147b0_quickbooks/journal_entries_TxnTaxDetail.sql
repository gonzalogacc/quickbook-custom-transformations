{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='journal_entries_TxnTaxDetail_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('journal_entries_TxnTaxDetail_ab3') }}
select
    _airbyte_journal_entries_hashid,
    TotalTax,
    TxnTaxCodeRef,
    TaxLine,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_TxnTaxDetail_hashid
from {{ ref('journal_entries_TxnTaxDetail_ab3') }}
-- TxnTaxDetail at journal_entries/TxnTaxDetail from {{ ref('journal_entries') }}
where 1 = 1

