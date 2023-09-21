

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_Line`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__deposits_Line_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_scd`

select
    _airbyte_deposits_hashid,
    json_extract_scalar(Line, "$['LineNum']") as LineNum,
    
        json_extract(Line, "$['DepositLineDetail']")
     as DepositLineDetail,
    json_extract_scalar(Line, "$['DetailType']") as DetailType,
    json_extract_scalar(Line, "$['Amount']") as Amount,
    json_extract_scalar(Line, "$['Id']") as Id,
    json_extract_array(Line, "$['LinkedTxn']") as LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_scd` as table_alias
-- Line at deposits/Line
cross join unnest(Line) as Line
where 1 = 1
and Line is not null

),  __dbt__cte__deposits_Line_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__deposits_Line_ab1
select
    _airbyte_deposits_hashid,
    cast(LineNum as 
    int64
) as LineNum,
    cast(DepositLineDetail as 
    string
) as DepositLineDetail,
    cast(DetailType as 
    string
) as DetailType,
    cast(Amount as 
    float64
) as Amount,
    cast(Id as 
    string
) as Id,
    LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__deposits_Line_ab1
-- Line at deposits/Line
where 1 = 1

),  __dbt__cte__deposits_Line_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__deposits_Line_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_deposits_hashid as 
    string
), ''), '-', coalesce(cast(LineNum as 
    string
), ''), '-', coalesce(cast(DepositLineDetail as 
    string
), ''), '-', coalesce(cast(DetailType as 
    string
), ''), '-', coalesce(cast(Amount as 
    string
), ''), '-', coalesce(cast(Id as 
    string
), ''), '-', coalesce(cast(array_to_string(LinkedTxn, "|", "") as 
    string
), '')) as 
    string
))) as _airbyte_Line_hashid,
    tmp.*
from __dbt__cte__deposits_Line_ab2 tmp
-- Line at deposits/Line
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__deposits_Line_ab3
select
    _airbyte_deposits_hashid,
    LineNum,
    DepositLineDetail,
    DetailType,
    Amount,
    Id,
    LinkedTxn,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_Line_hashid
from __dbt__cte__deposits_Line_ab3
-- Line at deposits/Line from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`deposits_scd`
where 1 = 1

  );
  