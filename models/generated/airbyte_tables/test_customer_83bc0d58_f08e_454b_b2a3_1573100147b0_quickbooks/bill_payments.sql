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
                            identifier='bill_payments_scd'
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
-- depends_on: {{ ref('bill_payments_ab3') }}
select
    CurrencyRef,
    ExchangeRate,
    TxnDate,
    airbyte_cursor,
    PayType,
    DepartmentRef,
    Line,
    SyncToken,
    CreditCardPayment,
    sparse,
    TotalAmt,
    MetaData,
    domain,
    DocNumber,
    APAccountRef,
    CheckPayment,
    Id,
    VendorRef,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_bill_payments_hashid
from {{ ref('bill_payments_ab3') }}
-- bill_payments from {{ source('test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks', '_airbyte_raw_bill_payments') }}
where 1 = 1

