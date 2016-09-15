drop table tt;
--drop table tt1;
--create table tt(lo integer not null, hi integer not null);
--copy tt from :inputfile delimiter ' ' null '' direct;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select a1 INTO TEMP TABLE tt ON COMMIT PRESERVE ROWS
--from :tablename AS foo1, (select * from tt) AS foo
--where cid>=foo.lo and cid<=foo.hi;
from :tablename
where cid>=0 and cid<2*2500*1000;
select count(*) from tt;
