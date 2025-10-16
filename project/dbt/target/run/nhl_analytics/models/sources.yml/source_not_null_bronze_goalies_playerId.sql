
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select playerId
from "nhl"."bronze"."goalies"
where playerId is null



  
  
      
    ) dbt_internal_test