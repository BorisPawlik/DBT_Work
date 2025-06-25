{{ config(materialized='incremental', unique_key='platform_id') }}

-- Récupération de la date de dernière modification si en mode incrémental
{% if is_incremental() %}
    with latest_modified as (
        select max(_modified) as max_modified
        from {{ this }}
    )
{% endif %}

-- Vue temporaire des plateformes distinctes
, distinct_values as (
    select distinct
        platform
    from {{ source('productivity', 'productivity') }}
    {% if is_incremental() %}
        where _modified > (select max_modified from latest_modified)
    {% endif %}
)

-- Final select avec génération d'un identifiant unique
select
    md5(cast(coalesce(cast(platform as string), '_dbt_utils_surrogate_key_null_') as string)) as platform_id,
    platform
from distinct_values
