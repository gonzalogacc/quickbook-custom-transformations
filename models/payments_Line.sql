

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__payments_Line_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_scd`

select
    _airbyte_payments_hashid,
    json_extract_scalar(Line, "$['Amount']") as Amount,
    
        json_extract(Line, "$['LineEx']")
     as LineEx,
    json_extract_array(Line, "$['LinkedTxn']") as LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_scd` as table_alias
-- Line at payments/Line
cross join unnest(Line) as Line
where 1 = 1
and Line is not null

),  __dbt__cte__payments_Line_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__payments_Line_ab1
select
    _airbyte_payments_hashid,
    cast(Amount as 
    float64
) as Amount,
    cast(LineEx as 
    string
) as LineEx,
    LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__payments_Line_ab1
-- Line at payments/Line
where 1 = 1

),  __dbt__cte__payments_Line_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__payments_Line_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_payments_hashid as 
    string
), ''), '-', coalesce(cast(Amount as 
    string
), ''), '-', coalesce(cast(LineEx as 
    string
), ''), '-', coalesce(cast(array_to_string(LinkedTxn, "|", "") as 
    string
), '')) as 
    string
))) as _airbyte_Line_hashid,
    tmp.*
from __dbt__cte__payments_Line_ab2 tmp
-- Line at payments/Line
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__payments_Line_ab3
select
    _airbyte_payments_hashid,
    Amount,
    LineEx,
    LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_Line_hashid
from __dbt__cte__payments_Line_ab3
-- Line at payments/Line from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_scd`
where 1 = 1

  );
  