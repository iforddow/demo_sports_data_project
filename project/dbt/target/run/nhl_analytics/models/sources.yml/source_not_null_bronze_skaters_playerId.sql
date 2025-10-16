
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select playerId
from "nhl"."bronze"."skaters"
where playerId is null



  
  
      
    ) dbt_internal_test