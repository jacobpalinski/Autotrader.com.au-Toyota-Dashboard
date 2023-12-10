select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      with relation_columns as (

        
        select
            cast('DATE' as STRING) as relation_column,
            cast('DATE' as STRING) as relation_column_type
        union all
        
        select
            cast('PRICE' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('ODOMETER' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('YEAR' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('CAR_MODEL' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('TYPE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('SUBURB' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('STATE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'PRICE'
            and
            relation_column_type not in ('INT64')

    )
    select *
    from test_data
      
    ) dbt_internal_test