

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line_LineEx`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__payments_Line_LineEx_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line`
select
    _airbyte_Line_hashid,
    json_extract_array(LineEx, "$['any']") as `any`,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line` as table_alias
-- LineEx at payments/Line/LineEx
where 1 = 1
and LineEx is not null

),  __dbt__cte__payments_Line_LineEx_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__payments_Line_LineEx_ab1
select
    _airbyte_Line_hashid,
    `any`,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__payments_Line_LineEx_ab1
-- LineEx at payments/Line/LineEx
where 1 = 1

),  __dbt__cte__payments_Line_LineEx_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__payments_Line_LineEx_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_Line_hashid as 
    string
), ''), '-', coalesce(cast(array_to_string(`any`, "|", "") as 
    string
), '')) as 
    string
))) as _airbyte_LineEx_hashid,
    tmp.*
from __dbt__cte__payments_Line_LineEx_ab2 tmp
-- LineEx at payments/Line/LineEx
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__payments_Line_LineEx_ab3
select
    _airbyte_Line_hashid,
    `any`,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_LineEx_hashid
from __dbt__cte__payments_Line_LineEx_ab3
-- LineEx at payments/Line/LineEx from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line`
where 1 = 1

  );
  