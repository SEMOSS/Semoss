# Database engines

Relational databases you can run alongside SEMOSS. Bring the container up first -
see [the engine index](README.md) for the compose commands and for how to address
a container from SEMOSS.

## ClickHouse

ClickHouse is a relational (OLAP) database, not a vector DB, so create it as a
database engine:

```
RDBMS_TYPE   CLICKHOUSE
HOSTNAME     semoss-clickhouse   (host: localhost)
PORT         8123
DATABASE     semoss
USERNAME     clickhouse
PASSWORD     clickhouse
```

ClickHouse has databases only, no schemas, so leave `SCHEMA` unset. Instead of
`HOSTNAME` / `PORT` / `DATABASE` you can set `CONNECTION_URL` directly to
`jdbc:clickhouse://semoss-clickhouse:8123/semoss`.

---

One of the [supporting engines](README.md).
