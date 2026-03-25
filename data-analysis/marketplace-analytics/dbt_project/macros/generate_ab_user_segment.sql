{% macro generate_ab_user_segment(segment_col) %}

    SELECT
        s.{{ segment_col }} AS segment,
        f.variant_user,
        COUNT(DISTINCT f.user_id) AS users,
        SUM(f.purchased) AS conversions,
        SUM(f.purchased) * 1.0 / COUNT(DISTINCT f.user_id) AS conversion_rate
    FROM {{ ref('fct_funnel_user') }} f
    JOIN {{ ref('segments') }} s ON f.user_id = s.user_id
    WHERE s.{{ segment_col }} IS NOT NULL
    GROUP BY 1, 2

{% endmacro %}
