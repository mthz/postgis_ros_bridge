
# restore database

in docker:

```
pg_restore -c -U postgres -d postgres_alchemy_ait -v /workspace/postgres_alchemy_ait.tar -W
```