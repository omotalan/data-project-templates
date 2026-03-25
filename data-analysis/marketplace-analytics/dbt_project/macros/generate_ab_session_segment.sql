{% macro generate_ab_session_segment(segment_col) %}

    SELECT
        s.{{ segment_col }} AS segment,
        f.variant_session,
        COUNT(DISTINCT f.user_session) AS user_sessions,
        SUM(f.purchased) AS conversions,
        SUM(f.purchased) * 1.0 / COUNT(DISTINCT f.user_session) AS conversion_rate
    FROM {{ ref('fct_funnel_session') }} f
    JOIN {{ ref('segments') }} s ON f.user_session = s.user_session
    WHERE s.{{ segment_col }} IS NOT NULL
    GROUP BY 1, 2

{% endmacro %}
