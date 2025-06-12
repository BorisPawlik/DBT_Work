SELECT
    {{ dbt_utils.generate_surrogate_key([
        'perceived_productivity_score',
        'actual_productivity_score'
    ]) }} AS productivite_id,
    perceived_productivity_score,
    actual_productivity_score
FROM (
    SELECT DISTINCT
        perceived_productivity_score,
        actual_productivity_score
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
