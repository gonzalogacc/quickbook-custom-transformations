

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line_LineEx_any_value`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__payments_Line_LineEx_any_value_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line_LineEx_any`
select
    _airbyte_any_hashid,
    json_extract_scalar(value, "$['Value']") as Value,
    json_extract_scalar(value, "$['Name']") as Name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line_LineEx_any` as table_alias
-- value at payments/Line/LineEx/any/value
where 1 = 1
and value is not null

),  __dbt__cte__payments_Line_LineEx_any_value_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__payments_Line_LineEx_any_value_ab1
select
    _airbyte_any_hashid,
    cast(Value as 
    string
) as Value,
    cast(Name as 
    string
) as Name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__payments_Line_LineEx_any_value_ab1
-- value at payments/Line/LineEx/any/value
where 1 = 1

),  __dbt__cte__payments_Line_LineEx_any_value_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__payments_Line_LineEx_any_value_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_any_hashid as 
    string
), ''), '-', coalesce(cast(Value as 
    string
), ''), '-', coalesce(cast(Name as 
    string
), '')) as 
    string
))) as _airbyte_value_hashid,
    tmp.*
from __dbt__cte__payments_Line_LineEx_any_value_ab2 tmp
-- value at payments/Line/LineEx/any/value
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__payments_Line_LineEx_any_value_ab3
select
    _airbyte_any_hashid,
    Value,
    Name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_value_hashid
from __dbt__cte__payments_Line_LineEx_any_value_ab3
-- value at payments/Line/LineEx/any/value from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`payments_Line_LineEx_any`
where 1 = 1

  );
  