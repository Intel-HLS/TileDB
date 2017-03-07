DROP LIBRARY SparseColumnReader CASCADE;
\t
\set libfile '\''`pwd`'/SparseColumnReader.so\'';
CREATE LIBRARY SparseColumnReader AS :libfile;

create transform function colRead
as language 'C++' name 'SparseColumnReaderFactory' library SparseColumnReader NOT FENCED;

select clear_caches();
select clear_caches();
select clear_caches();
\timing

\o
--CREATE TABLE XnY AS
--SELECT *
--FROM (SELECT colRead(x,y) OVER(PARTITION BEST) FROM vessel_traffic) AS sq where x >= 100000000 and x < 180000000 and y >= 100000000;
SELECT colRead(x,y) OVER(PARTITION BEST) FROM vessel_traffic;

\t
--SELECT COUNT(*) FROM XnY;
--DROP TABLE XnY;

DROP LIBRARY SparseColumnReader CASCADE;

--drop table t1;
--select clear_caches();
--\timing
--select x,y INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from vessel_traffic where x >= 100000000 and x < 180000000 and y >= 100000000;
--select count(*) from t1;
