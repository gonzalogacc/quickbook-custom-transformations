{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='credit_memos_scd'
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
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('credit_memos_ab3') }}
select
    CurrencyRef,
    ExchangeRate,
    ClassRef,
    EmailStatus,
    ShipAddr,
    RemainingCredit,
    HomeTotalAmt,
    MetaData,
    DocNumber,
    PrintStatus,
    BillEmail,
    BillAddr,
    TxnDate,
    airbyte_cursor,
    CustomerMemo,
    Line,
    SyncToken,
    sparse,
    TotalAmt,
    domain,
    CustomField,
    SalesTermRef,
    Id,
    CustomerRef,
    Balance,
    ApplyTaxAfterDiscount,
    TxnTaxDetail,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_credit_memos_hashid
from {{ ref('credit_memos_ab3') }}
-- credit_memos from {{ source('test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks', '_airbyte_raw_credit_memos') }}
where 1 = 1

