{{ config(materialized='incremental', unique_key='user_id') }}

-- Récupération de la date de dernière mise à jour de la table cible (si elle existe)
{% if is_incremental() %}
    with latest_modified as (
        select max(_modified) as max_modified
        from {{ this }}
    )
{% endif %}

-- Vue temporaire avec les données distinctes à insérer
, distinct_values as (
    select distinct
        user_name
    from {{ source('productivity', 'productivity') }}
    {% if is_incremental() %}
        where _modified > (select max_modified from latest_modified)
    {% endif %}
)

select
    md5(cast(coalesce(cast(user_name as string), '_dbt_utils_surrogate_key_null_') as string)) as user_id,
    user_name
from distinct_values
