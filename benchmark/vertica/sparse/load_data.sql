select clear_caches();
select clear_caches();
select clear_caches();
\timing
COPY :tablename FROM :input_file DELIMITER ' ' NULL '' DIRECT;
