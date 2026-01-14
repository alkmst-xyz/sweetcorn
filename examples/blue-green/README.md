# Blue Green Deployment Demo

## TODO

- [ ] Create script to start/stop containers; parse arguments to
      choose duckdb or ducklake as storage.
- [ ] Use docker compose profiles to selectively start services.
  - Useful to prevent starting ducklake dependencies when running in
    duckdb mode.
