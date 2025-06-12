SELECT
    {{ dbt_utils.generate_surrogate_key(['age', 'gender']) }} AS user_id,
    age,
    gender
FROM (
    SELECT DISTINCT
        age,
        gender
    FROM {{ source('dbt_productivity', 'productivity') }}
) AS distinct_values
