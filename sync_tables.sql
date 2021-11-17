create or replace procedure sync_tables(src STRING, trg STRING, index_col STRING)
BEGIN
    DECLARE src string;

    declare trg string;

    declare index_col string;

    DECLARE src_cols string;
    DECLARE trg_cols string;
    DECLARE t struct <proj string, ds string, tbl string>;
    DECLARE s struct <proj string, ds string, tbl string>;
    DECLARE trg_ds string;
    DECLARE trg_tbl string;
    DECLARE trg_proj string;
    set src = 'deft-melody-255020.cached.datasession_data';
    set trg = 'gfn-owox.gfn_source.datasession_data';
    set index_col = 'SessionIdHash';
    set t = (select as struct split(trg, '.')[offset(0)] as proj, split(trg, '.')[offset(1)] as ds,
                 split(trg, '.')[offset(2)] as tbl);
    set s = (select as struct split(src, '.')[offset(0)] as proj, split(src, '.')[offset(1)] as ds,
                 split(src, '.')[offset(2)] as tbl);
    execute immediate ((SELECT format(
            "SELECT STRING_AGG(column_name,',') FROM (select column_name from`%s.%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='%s' and table_schema='%s' order by column_name)",
            s.proj, s.ds, s.tbl, s.ds))) into src_cols;
    execute immediate ((SELECT format(
            "SELECT STRING_AGG(column_name,',') FROM (select column_name from`%s.%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='%s' and table_schema='%s' order by column_name)",
            t.proj, t.ds, t.tbl, t.ds))) into trg_cols;
    select src_cols;
    select (FORMAT("CREATE TABLE IF NOT EXISTS `%t` as (SELECT %t FROM %s LIMIT 0)", trg, src_cols, src));
    BEGIN
        EXECUTE IMMEDIATE format("insert into %t (%t) (select %t from `%t` where %t not in (select %t from `%t`))", trg,
                                 trg_cols, src_cols, src, index_col, index_col, trg);
    EXCEPTION
        WHEN ERROR THEN
        EXECUTE IMMEDIATE FORMAT("CREATE OR REPLACE TABLE `%t` as (SELECT %t FROM %s)", trg, src_cols, src);
    end;

END;

