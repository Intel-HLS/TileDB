-- 17536 rows
drop table t1;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select X,Y INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from vessel_traffic_with_tileid_10Kx10K where x<53900000 and y>=130000000 and y<136000000;
\timing
select count(*) from t1;
