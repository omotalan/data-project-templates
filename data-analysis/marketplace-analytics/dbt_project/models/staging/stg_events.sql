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

    brand,
    category_code,

    -- Chunking metadata logic
    parquet_scan_metadata('filename') AS chunk_filename,        -- DuckDB magic
    row_number() OVER (PARTITION BY chunk_filename ORDER BY event_time) AS chunk_row

-- Which one of the two FROMs is correct?
FROM read_parquet('data/chunks/chunk_*.parquet')
--FROM {{ source('raw', 'events') }}