-- Gebruikt om HASH toe te voegen aan kolom sales_detail.row_hash
update sales_detail
    set row_hash = hash(*)

-- Selecteer alle rijen uit een stream
select * from test_cdc;