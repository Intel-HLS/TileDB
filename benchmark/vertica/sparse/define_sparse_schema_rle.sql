CREATE TABLE vessel_traffic_rle (
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (X,Y)
);

CREATE TABLE vessel_traffic_3_rle (
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (X,Y)
);

CREATE TABLE vessel_traffic_5_rle (
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (X,Y)
);

CREATE TABLE vessel_traffic_with_tileid_10Kx10K_rle (
ID				INTEGER NOT NULL,
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (ID,X,Y)
);

CREATE TABLE vessel_traffic_with_tileid_10Kx10K_3_rle (
ID				INTEGER NOT NULL,
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (ID,X,Y)
);

CREATE TABLE vessel_traffic_with_tileid_10Kx10K_5_rle (
ID				INTEGER NOT NULL,
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (ID,X,Y)
);

CREATE TABLE vessel_traffic_with_tileid_100Kx100K_rle (
ID				INTEGER NOT NULL,
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (ID,X,Y)
);

CREATE TABLE vessel_traffic_with_tileid_1Mx1M_rle (
ID				INTEGER NOT NULL,
X         INTEGER NOT NULL,
Y         INTEGER NOT NULL,
SOG       INTEGER,
COG       INTEGER,
Heading   INTEGER,
ROT       INTEGER,
Status    INTEGER,
VoyageID  INTEGER,
MMSI      INTEGER,
PRIMARY KEY (ID,X,Y)
);
