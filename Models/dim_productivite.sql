WITH source AS (
    SELECT DISTINCT
        perceived_productivity_score,
        actual_productivity_score
    FROM {{ source('dbt_productivity', 'productivity') }}
),

with_ids AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['perceived_productivity_score', 'actual_productivity_score']) }} AS productivite_id,
        perceived_productivity_score,
        actual_productivity_score
    FROM source
)

SELECT * FROM with_ids;
