-- 17536 rows
drop table t1;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select X,Y INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from vessel_traffic_with_tileid where x>=48300000 and x<85227100 and y>=115778980 and y<122337000;
\timing
select count(*) from t1;
