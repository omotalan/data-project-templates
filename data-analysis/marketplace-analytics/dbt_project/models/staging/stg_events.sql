SELECT
    user_id,
    product_id,
    event_time,
    event_type,

    -- Normalize event naming (important for storytelling later)
    CASE 
        WHEN event_type = 'view' THEN 'view_item'
        WHEN event_type = 'cart' THEN 'add_to_cart'
        WHEN event_type = 'purchase' THEN 'contact_seller'
        ELSE event_type
    END AS event_name,

    CASE 
        WHEN user_id % 2 = 0 THEN 'A'
        ELSE 'B'
    END AS variant,
    
    -- Pulled from raw events so it can feed segments.sql downstream. Other columns could be pulled on a need basis
    brand,
    category_code

FROM {{ source('raw', 'events') }}