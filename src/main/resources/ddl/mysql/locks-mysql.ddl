-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.locks
(
    id        BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `key`     VARCHAR(255) NOT NULL UNIQUE,
    locked_at TIMESTAMP   NOT NULL DEFAULT NOW()
) ENGINE = InnoDB;

SET @x := (SELECT COUNT(*)
           FROM information_schema.statistics
           WHERE table_name = 'locks'
             AND index_name = 'locks_key_idx'
             AND table_schema = DATABASE());
SET @sql := if(@x > 0, 'select ''idx exists''', 'ALTER TABLE locks ADD INDEX locks_key_idx (`key`);');
PREPARE stmt FROM @sql;
EXECUTE stmt;