

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_RemitToAddr`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__purchases_RemitToAddr_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_scd`
select
    _airbyte_purchases_hashid,
    json_extract_scalar(RemitToAddr, "$['CountrySubDivisionCode']") as CountrySubDivisionCode,
    json_extract_scalar(RemitToAddr, "$['Long']") as Long,
    json_extract_scalar(RemitToAddr, "$['PostalCode']") as PostalCode,
    json_extract_scalar(RemitToAddr, "$['Id']") as Id,
    json_extract_scalar(RemitToAddr, "$['City']") as City,
    json_extract_scalar(RemitToAddr, "$['Line1']") as Line1,
    json_extract_scalar(RemitToAddr, "$['Lat']") as Lat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_scd` as table_alias
-- RemitToAddr at purchases/RemitToAddr
where 1 = 1
and RemitToAddr is not null

),  __dbt__cte__purchases_RemitToAddr_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__purchases_RemitToAddr_ab1
select
    _airbyte_purchases_hashid,
    cast(CountrySubDivisionCode as 
    string
) as CountrySubDivisionCode,
    cast(Long as 
    string
) as Long,
    cast(PostalCode as 
    string
) as PostalCode,
    cast(Id as 
    string
) as Id,
    cast(City as 
    string
) as City,
    cast(Line1 as 
    string
) as Line1,
    cast(Lat as 
    string
) as Lat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__purchases_RemitToAddr_ab1
-- RemitToAddr at purchases/RemitToAddr
where 1 = 1

),  __dbt__cte__purchases_RemitToAddr_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__purchases_RemitToAddr_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_purchases_hashid as 
    string
), ''), '-', coalesce(cast(CountrySubDivisionCode as 
    string
), ''), '-', coalesce(cast(Long as 
    string
), ''), '-', coalesce(cast(PostalCode as 
    string
), ''), '-', coalesce(cast(Id as 
    string
), ''), '-', coalesce(cast(City as 
    string
), ''), '-', coalesce(cast(Line1 as 
    string
), ''), '-', coalesce(cast(Lat as 
    string
), '')) as 
    string
))) as _airbyte_RemitToAddr_hashid,
    tmp.*
from __dbt__cte__purchases_RemitToAddr_ab2 tmp
-- RemitToAddr at purchases/RemitToAddr
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__purchases_RemitToAddr_ab3
select
    _airbyte_purchases_hashid,
    CountrySubDivisionCode,
    Long,
    PostalCode,
    Id,
    City,
    Line1,
    Lat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_RemitToAddr_hashid
from __dbt__cte__purchases_RemitToAddr_ab3
-- RemitToAddr at purchases/RemitToAddr from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`purchases_scd`
where 1 = 1

  );
  