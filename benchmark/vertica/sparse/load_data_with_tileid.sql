select clear_caches();
\timing
COPY vessel_traffic_with_tileid_100Kx100K FROM :input_file DELIMITER ' ' NULL '' DIRECT;
