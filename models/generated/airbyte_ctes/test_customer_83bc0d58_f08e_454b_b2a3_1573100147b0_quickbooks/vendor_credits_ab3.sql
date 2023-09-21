{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_test_customer_83bc0d58_f08e_454b_b2a3_1573100147b0_quickbooks",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('vendor_credits_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        object_to_string('CurrencyRef'),
        'ExchangeRate',
        'TxnDate',
        'airbyte_cursor',
        object_to_string('DepartmentRef'),
        array_to_string('Line'),
        'SyncToken',
        'TotalAmt',
        object_to_string('MetaData'),
        'domain',
        'DocNumber',
        object_to_string('APAccountRef'),
        'Id',
        object_to_string('VendorRef'),
    ]) }} as _airbyte_vendor_credits_hashid,
    tmp.*
from {{ ref('vendor_credits_ab2') }} tmp
-- vendor_credits
where 1 = 1

