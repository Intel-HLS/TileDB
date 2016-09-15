-- 17536 rows
drop table t1;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select X,Y INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from vessel_traffic_with_tileid where x>=62000000 and x<64900000 and y>=114000000 and y<122400000;
select count(*) from t1;
