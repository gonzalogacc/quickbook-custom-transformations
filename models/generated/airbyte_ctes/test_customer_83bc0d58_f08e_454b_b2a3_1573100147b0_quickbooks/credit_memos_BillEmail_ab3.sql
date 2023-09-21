{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('credit_memos_BillEmail_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte_credit_memos_hashid',
        'Address',
    ]) }} as _airbyte_BillEmail_hashid,
    tmp.*
from {{ ref('credit_memos_BillEmail_ab2') }} tmp
-- BillEmail at credit_memos/BillEmail
where 1 = 1

