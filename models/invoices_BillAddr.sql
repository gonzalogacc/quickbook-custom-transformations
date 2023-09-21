

  create or replace table `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_BillAddr`
  partition by timestamp_trunc(_airbyte_emitted_at, day)
  cluster by _airbyte_emitted_at
  OPTIONS()
  as (
    
with __dbt__cte__invoices_BillAddr_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_scd`
select
    _airbyte_invoices_hashid,
    json_extract_scalar(BillAddr, "$['Line4']") as Line4,
    json_extract_scalar(BillAddr, "$['CountrySubDivisionCode']") as CountrySubDivisionCode,
    json_extract_scalar(BillAddr, "$['Long']") as Long,
    json_extract_scalar(BillAddr, "$['PostalCode']") as PostalCode,
    json_extract_scalar(BillAddr, "$['Id']") as Id,
    json_extract_scalar(BillAddr, "$['City']") as City,
    json_extract_scalar(BillAddr, "$['Line1']") as Line1,
    json_extract_scalar(BillAddr, "$['Lat']") as Lat,
    json_extract_scalar(BillAddr, "$['Line2']") as Line2,
    json_extract_scalar(BillAddr, "$['Line3']") as Line3,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_scd` as table_alias
-- BillAddr at invoices/BillAddr
where 1 = 1
and BillAddr is not null

),  __dbt__cte__invoices_BillAddr_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__invoices_BillAddr_ab1
select
    _airbyte_invoices_hashid,
    cast(Line4 as 
    string
) as Line4,
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
    cast(Line2 as 
    string
) as Line2,
    cast(Line3 as 
    string
) as Line3,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at
from __dbt__cte__invoices_BillAddr_ab1
-- BillAddr at invoices/BillAddr
where 1 = 1

),  __dbt__cte__invoices_BillAddr_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__invoices_BillAddr_ab2
select
    to_hex(md5(cast(concat(coalesce(cast(_airbyte_invoices_hashid as 
    string
), ''), '-', coalesce(cast(Line4 as 
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
), ''), '-', coalesce(cast(Line2 as 
    string
), ''), '-', coalesce(cast(Line3 as 
    string
), '')) as 
    string
))) as _airbyte_BillAddr_hashid,
    tmp.*
from __dbt__cte__invoices_BillAddr_ab2 tmp
-- BillAddr at invoices/BillAddr
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__invoices_BillAddr_ab3
select
    _airbyte_invoices_hashid,
    Line4,
    CountrySubDivisionCode,
    Long,
    PostalCode,
    Id,
    City,
    Line1,
    Lat,
    Line2,
    Line3,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP() as _airbyte_normalized_at,
    _airbyte_BillAddr_hashid
from __dbt__cte__invoices_BillAddr_ab3
-- BillAddr at invoices/BillAddr from `honu-dev-1`.test_customer_b2cf8d57_842e_4971_86af_716a17d07cd3_quickbooks.`invoices_scd`
where 1 = 1

  );
  