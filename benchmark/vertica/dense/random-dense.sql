drop table tt;
drop table tt1;
create table tt(tileid integer not null, x integer not null, y integer not null);
copy tt from :inputfile delimiter ' ' null '' direct;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select a1 INTO TEMP TABLE tt1 ON COMMIT PRESERVE ROWS from :tablename where (x,y) in (select x,y from tt where tt.x=x and tt.y=y);
