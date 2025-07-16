SELECT
    p.pid,
    a.query_start,
    now() - a.query_start AS duration,
    p.phase,
    p.blocks_total,
    p.blocks_done,
    p.tuples_total,
    p.tuples_done
FROM
    pg_stat_progress_create_index p
JOIN
    pg_stat_activity a ON p.pid = a.pid
WHERE
    p.command = 'CREATE INDEX CONCURRENTLY';

SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size
FROM pg_indexes
WHERE tablename = 'history_provideraudithistory'
ORDER BY pg_relation_size(indexname::regclass) DESC;
