package storage

const (
	DefaultDuckLakeName = "sweetcorn_ducklake"

	installDuckLakeSQL = `INSTALL ducklake;`
	installPostgresSQL = `INSTALL postgres;`
	createS3SecretSQL  = `CREATE OR REPLACE SECRET (
		TYPE s3,
		PROVIDER config,
		KEY_ID 'minio-user',
		SECRET 'minio-secret',
		REGION 'us-east-1',
		ENDPOINT '127.0.0.1:9000',
		URL_STYLE 'path',
		USE_SSL false
	);`
	createPostgresSecretSQL = `CREATE OR REPLACE SECRET (
		TYPE postgres,
		HOST '127.0.0.1',
		PORT 5432,
		DATABASE postgres,
		USER 'admin',
		PASSWORD 'admin'
	);`
	attachDuckLakeSQL = `ATTACH 'ducklake:postgres:dbname=postgres' AS %s (DATA_PATH 's3://sweetcorn/');`
	useDuckLakeSQL    = `USE %s;`
)
