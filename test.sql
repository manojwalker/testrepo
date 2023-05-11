    select distinct
        query_id
      , warehouse_id
      , warehouse_name
      ,schema_id
      ,schema_name
      , original_start_time
      , pseudo_start_time
      , coalesce(
          lead(pseudo_start_time) over(partition by query_id order by pseudo_start_time)
          , end_time
        ) as end_time
      , query_type
      , session_id
      , user_name
    from row_gen) as job
      left join
      (
        select to_char(start_time, 'YYYY-MM-DD HH24') as metering_hour
          , warehouse_id
          , sum(credits_used) as total_credits
        from snowflake.account_usage.warehouse_metering_history
        group by 1,2
      ) as credit_usage
      on job.warehouse_id = credit_usage.warehouse_id
      and to_char(job.pseudo_start_time, 'YYYY-MM-DD HH24') = credit_usage.metering_hour
      )
    group by 1