# Blue Green Deployment Demo

| Service       | Endpoint                      |
| ------------- | ----------------------------- |
| Hot R.O.D.    | http://localhost:8080         |
| Grafana       | http://localhost:3000         |
| Sweetcorn UI  | http://localhost:13579        |
| Jaeger UI     | http://localhost:16686/search |
| Prometheus UI | http://localhost:9090/query   |

## TODO

- [ ] Create script to start/stop containers; parse arguments to
      choose duckdb or ducklake as storage.
- [ ] Use docker compose profiles to selectively start services.
  - Useful to prevent starting ducklake dependencies when running in
    duckdb mode.
