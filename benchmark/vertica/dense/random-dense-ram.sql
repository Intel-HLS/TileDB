drop table tt; 
drop table tt1;
drop table tt2;
create table tt(tileid integer not null, x integer not null, y integer not null);
create table tt2(cid integer not null);
copy tt from :inputfile delimiter ' ' null '' direct;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
insert into tt2 select (tt.tileid*2500*1000+(tt.x%2500)*1000+(tt.y%1000)) from tt;
select a1 INTO TEMP TABLE tt1 ON COMMIT PRESERVE ROWS
from :tablename
where (cid) in (select cid from tt2 where cid=tt2.cid);
select count(*) from tt1;
