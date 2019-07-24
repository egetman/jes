CREATE SCHEMA IF NOT EXISTS stock;

CREATE SEQUENCE IF NOT EXISTS stock.item_sequence
    MINVALUE 1
    INCREMENT BY 50
    NO CYCLE;

CREATE TABLE IF NOT EXISTS stock.item
(
    id           BIGINT      NOT NULL PRIMARY KEY DEFAULT nextval('stock.item_sequence'),
    uuid         UUID        NOT NULL,
    name         VARCHAR(50) NOT NULL,
    quantity     BIGINT      NOT NULL,
    last_ordered TIMESTAMP
);