drop table t1;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select A1 INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from :tablename where x>=1 and x<2499 and y>=1 and y<999;
select count(*) from t1;
