select distinct
    job_id,
    work_hours_per_day,
    break_during_work
from {{ source('dbt_demo', 'productivity') }}
