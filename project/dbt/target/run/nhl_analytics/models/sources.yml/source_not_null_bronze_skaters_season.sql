
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season
from "nhl"."bronze"."skaters"
where season is null



  
  
      
    ) dbt_internal_test