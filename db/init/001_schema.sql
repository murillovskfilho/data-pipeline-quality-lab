-- Medallion layout for a small shipment-tracking lake.
--   bronze: raw, append-only copy of every Kafka message (including garbage)
--   silver: validated, deduplicated, typed entities
--   gold:   business aggregates rebuilt from silver

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

CREATE TABLE bronze.shipment_events (
    id              bigserial PRIMARY KEY,
    topic           text        NOT NULL,
    kafka_partition int         NOT NULL,
    kafka_offset    bigint      NOT NULL,
    message_key     text,
    raw_value       text        NOT NULL,
    payload         jsonb,                      -- NULL when the message is not valid JSON
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (topic, kafka_partition, kafka_offset)
);
CREATE INDEX ON bronze.shipment_events ((payload ->> 'shipment_id'));
CREATE INDEX ON bronze.shipment_events ((payload ->> 'event_id'));

CREATE TABLE silver.shipments (
    shipment_id      text PRIMARY KEY,
    tenant_id        text          NOT NULL,
    carrier          text          NOT NULL,
    service_level    text,
    origin_zip       text,
    destination_zip  text,
    weight_oz        numeric(10, 2),
    status           text          NOT NULL,
    label_created_at timestamptz,
    delivered_at     timestamptz,
    last_event_at    timestamptz   NOT NULL,
    updated_at       timestamptz   NOT NULL DEFAULT now()
);
CREATE INDEX ON silver.shipments (tenant_id);

-- No FK to silver.shipments on purpose: referential integrity is asserted by a data-quality check.
CREATE TABLE silver.tracking_events (
    event_id     text PRIMARY KEY,
    shipment_id  text        NOT NULL,
    tenant_id    text        NOT NULL,
    event_type   text        NOT NULL,
    occurred_at  timestamptz NOT NULL,
    bronze_id    bigint      NOT NULL REFERENCES bronze.shipment_events (id),
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON silver.tracking_events (shipment_id);
CREATE INDEX ON silver.tracking_events (tenant_id);
CREATE INDEX ON silver.tracking_events (bronze_id);

-- Quarantine: everything bronze received but silver refused, with the reason.
CREATE TABLE silver.rejected_events (
    bronze_id   bigint PRIMARY KEY REFERENCES bronze.shipment_events (id),
    reason      text        NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE gold.carrier_daily_performance (
    day                 date           NOT NULL,
    tenant_id           text           NOT NULL,
    carrier             text           NOT NULL,
    shipments_delivered int            NOT NULL,
    p50_transit_hours   numeric(10, 2) NOT NULL,
    p95_transit_hours   numeric(10, 2) NOT NULL,
    refreshed_at        timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (day, tenant_id, carrier)
);

CREATE TABLE gold.refresh_log (
    id           bigserial PRIMARY KEY,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    rows_written int         NOT NULL
);
