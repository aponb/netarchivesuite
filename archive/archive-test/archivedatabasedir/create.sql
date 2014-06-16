CREATE TABLE "APP"."FILE"( FILE_ID bigint PRIMARY KEY NOT NULL, FILENAME varchar(300))
CREATE TABLE "APP"."REPLICA"( REPLICA_GUID bigint PRIMARY KEY NOT NULL, REPLICA_ID varchar(300) NOT NULL, REPLICA_NAME varchar(300) NOT NULL, REPLICA_TYPE varchar(50) NOT NULL, FILELIST_UPDATED timestamp, CHECKSUM_UPDATED timestamp)
CREATE TABLE "APP"."REPLICAFILEINFO"( REPLICAFILEINFO_GUID bigint PRIMARY KEY NOT NULL, REPLICA_ID varchar(300) NOT NULL, FILE_ID bigint, SEGMENT_ID bigint, CHECKSUM varchar(300), UPLOAD_STATUS int, CHECKSUM_STATUS int, FILELIST_STATUS int, FILELIST_CHECKDATETIME timestamp, CHECKSUM_CHECKDATETIME timestamp)
CREATE TABLE "APP"."SCHEMAVERSIONS"( TABLENAME varchar(100) NOT NULL, VERSION int NOT NULL)
CREATE TABLE "APP"."SEGMENT"( SEGMENT_GUID bigint PRIMARY KEY NOT NULL, REPLICA_ID bigint, SEGMENT_ID bigint, SEGMENT_ADDRESS varchar(300), FILELIST_CHECKDATETIME timestamp, CHECKSUM_CHECKDATETIME timestamp)
