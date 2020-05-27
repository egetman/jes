-- noinspection SqlResolveForFile
-- schemaName, contentType & schemaPassword are aliases. They will be replaced with actual types at runtime.
-- ddl for oracle < 12
DECLARE
    user_exists INTEGER;
    table_exists INTEGER;
BEGIN
    SELECT COUNT(*) INTO user_exists FROM dba_users WHERE username = 'schemaName';
    SELECT COUNT(*) INTO table_exists FROM dba_tables WHERE table_name = 'EVENT_STORE' OR table_name = 'event_store';

    IF (user_exists = 0) THEN
        EXECUTE IMMEDIATE 'CREATE USER schemaName IDENTIFIED BY schemaPassword';
    END IF;

    IF (table_exists = 0) THEN
        EXECUTE IMMEDIATE '
        CREATE TABLE schemaName.event_store
        (
            id   NUMBER PRIMARY KEY,
            uuid RAW(16),
            data contentType NOT NULL
        )';
        EXECUTE IMMEDIATE 'CREATE INDEX uuid_idx ON schemaName.event_store(uuid)';
        EXECUTE IMMEDIATE 'CREATE SEQUENCE event_store_sequence';
        EXECUTE IMMEDIATE
            'CREATE OR REPLACE TRIGGER event_store_insert
                BEFORE INSERT ON schemaName.event_store
                FOR EACH ROW
                    BEGIN
                        SELECT event_store_sequence.nextval
                        INTO :new.id
                        FROM dual;
                    END;';
    END IF;
END;