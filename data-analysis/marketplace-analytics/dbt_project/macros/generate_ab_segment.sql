{% macro generate_ab_segment(segment_col) %}

    SELECT
        s.{{ segment_col }} AS segment,
        f.variant,
        COUNT(DISTINCT f.user_id) AS users,
        SUM(f.contacted) AS conversions,
        SUM(f.contacted) * 1.0 / COUNT(DISTINCT f.user_id) AS conversion_rate
    FROM {{ ref('fct_funnel') }} f
    JOIN {{ ref('segments') }} s ON f.user_id = s.user_id
    WHERE s.{{ segment_col }} IS NOT NULL
    GROUP BY 1, 2

{% endmacro %}
