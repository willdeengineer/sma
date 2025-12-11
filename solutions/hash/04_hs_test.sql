CALL insert_if_different(
    'sales_detail',
    OBJECT_CONSTRUCT(
        'SALE_DETAIL_ID', 1,
        'SALE_ID', 5000,
        'PRODUCT_ID', 101,
        'QUANTITY', 2,
        'UNIT_PRICE', 15.00,
        'LINE_TOTAL', 30.00,
        'CREATED_AT', CURRENT_TIMESTAMP()
    )
);