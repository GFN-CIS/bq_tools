create or replace procedure sync_tables(src STRING, trg STRING, index_col STRING, partition_clause STRING)
BEGIN
    DECLARE src_cols string;
    DECLARE t struct <proj string, ds string, tbl string>;
    DECLARE s struct <proj string, ds string, tbl string>;
    DECLARE temp_trg string;
    DECLARE sync_suff string;

    set t = (select as struct split(trg, '.')[offset(0)] as proj, split(trg, '.')[offset(1)] as ds,
                 split(trg, '.')[offset(2)] as tbl);
    set s = (select as struct split(src, '.')[offset(0)] as proj, split(src, '.')[offset(1)] as ds,
                 split(src, '.')[offset(2)] as tbl);
    set sync_suff = (FORMAT('_%t', TO_HEX(MD5(trg))));
    SET temp_trg = (FORMAT('%t.%t.st_%t_%t', t.proj, t.ds, t.tbl, sync_suff));
    execute immediate ((SELECT format(
            "SELECT STRING_AGG(column_name,',') FROM (select column_name from`%s.%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='%s' and table_schema='%s' order by column_name)",
            s.proj, s.ds, s.tbl, s.ds))) into src_cols;
    BEGIN
        EXECUTE IMMEDIATE format(
                "insert into `%t` (%t, ts_%t) (select %t, CURRENT_TIMESTAMP() as ts_%t from `%t` where %t not in (select %t from `%t`))",
                temp_trg, src_cols, sync_suff, src_cols, sync_suff, src, index_col, index_col, temp_trg);
    EXCEPTION
        WHEN ERROR THEN
        EXECUTE IMMEDIATE FORMAT(
                "CREATE OR REPLACE TABLE `%t` %t as (SELECT %t, CURRENT_TIMESTAMP() as ts_%t FROM %s)", temp_trg,
                 IF(partition_clause is not null, CONCAT("PARTITION BY ", partition_clause), ""), src_cols, sync_suff, src);
    end;
    EXECUTE IMMEDIATE format("""CREATE VIEW IF NOT EXISTS %t as
    (select %t from (select %t, ROW_NUMBER() OVER (partition by %t order by ts_%t desc) as rn_%t from %t) where rn_%t=1)""",
                             trg, src_cols, src_cols, index_col, sync_suff, sync_suff, temp_trg, sync_suff);
END;

