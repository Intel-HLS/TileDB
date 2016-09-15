-- 17536 rows
drop table t1;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select X,Y INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from vessel_traffic_with_tileid where x>=99000000 and x<100000000 and y>=116000000 and y<132600000;
select count(*) from t1;
