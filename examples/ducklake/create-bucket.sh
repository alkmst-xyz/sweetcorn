#!/bin/bash

set -e

until mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD; do
	echo "waiting for minio server"
	sleep 1
done

mc mb --ignore-existing local/sweetcorn
