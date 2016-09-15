drop table tt;
--drop table tt1;
--create table tt(lo integer not null);
--copy tt from :inputfile delimiter ' ' null '' direct;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select a1 INTO TEMP TABLE tt ON COMMIT PRESERVE ROWS
--from :tablename AS foo1, (select * from tt) AS foo
--where cid=foo.lo;
from :tablename
where cid%20000=0;
select count(*) from tt;
