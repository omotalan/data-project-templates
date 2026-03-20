{{ config(materialized='incremental')}}

SELECT
    variant,
    count(distinct user_id) AS users,
    sum(contacted) AS conversions,
    -- Force float division
    sum(contacted) * (1.0 / count(distinct user_id)) AS user_conversion_rate,
    sum(contacted) * (1.0 / sum(viewed)) AS view_conversion_rate
FROM {{ ref('fct_funnel') }}
GROUP BY 1

{% if is_incremental() %}
    -- Only new chunks by filename pattern
    WHERE chunk_id > (SELECT max(chunk_id) FROM {{ this}})
{% endif %}