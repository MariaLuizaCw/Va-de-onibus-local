# Va-de-onibus Backend

## Environment Variables

The backend relies on the following environment variables. Defaults are applied when noted:

| Variable | Description | Default |
| --- | --- | --- |
| `DATABASE_HOST` | PostgreSQL host address | required |
| `DATABASE_PORT` | PostgreSQL port | `5432` if unset |
| `DATABASE_NAME` | Database name | required |
| `DATABASE_USER` | Database user | required |
| `DATABASE_PASSWORD` | Database password | required |
| `BACKEND_PORT` | Port that the backend server listens on | `3001` |
| `API_TIMEZONE` | Time zone used when creating reports and partitions | `America/Sao_Paulo` |
| `PARTITION_RETENTION_DAYS` | How many days of partitions to keep | `7` |
| `PARTITION_CHECK_INTERVAL_MS` | Frequency of automatic partition creation and deletion| `86400000` (24h) |
| `RIO_POLLING_INTERVAL_MS` | How often Rio GPS data is polled | `60000` |
| `RIO_POLLING_WINDOW_MINUTES` | Lookback window for Rio catchup requests in minutes | `3` |
| `CATCHUP_HOURS` | Hours of historical Rio data to fetch at startup | `1` |
| `ANGRA_POLLING_INTERVAL_MS` | Interval to poll the Angra SSX API | `60000` |
| `ANGRA_CIRCULAR_LINES_POLL_MS` | How often cached Angra circular lines are refreshed | `86400000` |
| `ANGRA_SSX_USERNAME` | Angra SSX API username | required |
| `ANGRA_SSX_PASSWORD` | Angra SSX API password | required |
| `ANGRA_SSX_CLIENT_CODE` | Angra SSX client integration code | required |
| `ANGRA_SSX_TOKEN_REFRESH_MS` | Force token refresh interval | `18000000` (5h) |
| `SNAPSHOT_INTERVAL_MS` | Interval to persist in-memory snapshots for Rio/Angra | `900000` |
| `COVERAGE_REPORT_INTERVAL_MS` | How often the coverage reports run | `86400000` |
| `MAX_SNAP_DISTANCE_METERS` | Maximum distance (meters) to match GPS points to itineraries | `300` |
| `SENTIDO_BATCH_SIZE` | Batch size used when enriching Rio records with sentido data | `2000` |
| `DB_BATCH_SIZE` | Batch size when inserting Rio/Angra GPS records | `2000` |