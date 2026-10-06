-- include/sql/raw_load.sql
-- ELT step 2: LOAD RAW. Exact copy of the CSV: every column text, empty cells stay '' (nullstr is a sentinel that
-- never occurs), no trimming, no casting, no filtering - bad rows are loaded like any other.
-- {table} is substituted by the loader (validated identifier); $-parameters are bound by DuckDB.

INSERT INTO {table}
SELECT t.*,
       current_timestamp::TIMESTAMP AS _loaded_at,
       $source_file                 AS _source_file,
       $run_id                      AS _run_id,
       CAST($batch_id AS BIGINT)    AS _batch_id,
       row_number() OVER ()         AS _source_row_num
FROM read_csv($path, all_varchar = true, header = true, nullstr = '<<NULL>>') AS t;
