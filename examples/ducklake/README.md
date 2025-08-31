# DuckLake

## Quickstart

```bash
docker compose up -d
```

| Service          | Link                   |
| ---------------- | ---------------------- |
| Minio Console UI | http://localhost:9090/ |

## SQL

```sql
INSTALL ducklake;
INSTALL postgres;

CREATE OR REPLACE SECRET (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minio-user',
    SECRET 'minio-secret',
    REGION 'us-east-1',
    ENDPOINT '0.0.0.0:9000',
    URL_STYLE 'path',
    USE_SSL false
);

CREATE OR REPLACE SECRET (
    TYPE postgres,
    HOST '0.0.0.0',
    PORT 5432,
    DATABASE postgres,
    USER 'admin',
    PASSWORD 'admin'
);

ATTACH 'ducklake:postgres:dbname=postgres' AS sweetcorn_ducklake (DATA_PATH 's3://sweetcorn/');

USE sweetcorn_ducklake;
```

## Partitioning

```sql
ALTER TABLE otel_logs
SET PARTITIONED BY (year(ts), month(ts), day(ts));

ALTER TABLE otel_traces
SET PARTITIONED BY (year(ts), month(ts), day(ts));
```
