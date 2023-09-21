{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('invoices_DeliveryInfo_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte_invoices_hashid',
        'DeliveryType',
    ]) }} as _airbyte_DeliveryInfo_hashid,
    tmp.*
from {{ ref('invoices_DeliveryInfo_ab2') }} tmp
-- DeliveryInfo at invoices/DeliveryInfo
where 1 = 1

