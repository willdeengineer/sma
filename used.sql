
-- Voegt een nieuwe kolom toe aan de tabel sales_detail voor het opslaan van hash
alter table sales_detail
    add column row_hash string;

-- Gebruikt om hash toe te voegen aan kolom sales_detail.row_hash
update sales_detail
    set row_hash = hash(*)
    where row_hash is null;

-- Selecteert alle rijen uit een stream
select * from stream_name;

PUT 'file://C:/Users/wmeijden/OneDrive - Capgemini/Desktop/customer.csv' @entity_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

LIST @entity_stage;

