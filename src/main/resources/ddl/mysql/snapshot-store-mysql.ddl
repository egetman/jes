-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.snapshot_store
(
    id   BIGINT     NOT NULL AUTO_INCREMENT PRIMARY KEY,
    uuid BINARY(80) NOT NULL,
    data TEXT       NOT NULL
) ENGINE = InnoDB;

SET @x := (SELECT COUNT(*)
           FROM information_schema.statistics
           WHERE table_name = 'snapshot_store'
             AND index_name = 'uuid_idx'
             AND table_schema = DATABASE());
SET @sql := if(@x > 0, 'select ''idx exists''', 'ALTER TABLE snapshot_store ADD INDEX uuid_idx (uuid);');
PREPARE stmt FROM @sql;
EXECUTE stmt;

-- todo: trim columns for better performance (for example uuid - binary(80) for mysql >.<)