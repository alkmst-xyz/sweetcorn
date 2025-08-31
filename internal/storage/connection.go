package storage

import (
	"context"
	"database/sql"
)

const (
	setupDuckLakeSQL = `
INSTALL ducklake;
INSTALL postgres;

CREATE OR REPLACE SECRET (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minio-user',
    SECRET 'minio-secret',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9000',
    URL_STYLE 'path',
    USE_SSL false
);

CREATE OR REPLACE SECRET (
    TYPE postgres,
    HOST '127.0.0.1',
    PORT 5432,
    DATABASE postgres,
    USER 'admin',
    PASSWORD 'admin'
);

ATTACH 'ducklake:postgres:dbname=postgres' AS sweetcorn_ducklake (DATA_PATH 's3://sweetcorn/');

USE sweetcorn_ducklake;`
)

func SetupDuckLake(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, setupDuckLakeSQL); err != nil {
		return err
	}
	return nil
}
