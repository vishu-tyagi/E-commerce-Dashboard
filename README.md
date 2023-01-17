# Ecommerce-Dashboard

## Instructions

#### Move into top-level directory
```
cd Ecommerce-Dashboard

```

#### Extract raw data
```
make extract

```

#### Ingest raw data into Postgres
```
make load \
    user=pg_user \
    password=pg_password \
    host=pg_host \
    port=pg_port \
    db=pg_db \
    schema=pg_schem

```

Example usage
```
make load \
    user=root \
    password=root \
    host=127.0.0.1 \
    port=5432 \
    db=ecom_sales \
    schema=staging

```
will create (if it does not exist) the schema `staging` in the Postgres database `ecom_sales` running on port `5432` and ingest the table(s) into it.