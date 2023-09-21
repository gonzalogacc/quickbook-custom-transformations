{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='vendor_credits_Line_scd'
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
-- depends_on: {{ ref('vendor_credits_Line_ab3') }}
select
    _airbyte_vendor_credits_hashid,
    LineNum,
    Description,
    AccountBasedExpenseLineDetail,
    DetailType,
    Amount,
    Id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_Line_hashid
from {{ ref('vendor_credits_Line_ab3') }}
-- Line at vendor_credits/Line from {{ ref('vendor_credits') }}
where 1 = 1

