{{ config(materialized='incremental', unique_key='job_id') }}

-- CTE pour récupérer la dernière date de modification (en mode incrémental)
{% if is_incremental() %}
    with latest_modified as (
        select max(_modified) as max_modified
        from {{ this }}
    )
{% endif %}

-- CTE pour extraire les valeurs distinctes
, distinct_values as (
    select distinct
        job
    from {{ source('productivity', 'productivity') }}
    {% if is_incremental() %}
        where _modified > (select max_modified from latest_modified)
    {% endif %}
)

-- Résultat final avec génération d'un identifiant unique
select
    md5(cast(coalesce(cast(job as string), '_dbt_utils_surrogate_key_null_') as string)) as job_id,
    job
from distinct_values
