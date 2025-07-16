#!/bin/bash

nohup bash -c "PGPASSWORD=$DATABASE_PASSWORD psql -U $DATABASE_USER -h $DATABASE_HOST -d $DATABASE_NAME -c 'CREATE INDEX CONCURRENTLY IF NOT EXISTS history_pro_update__9dff21_idx ON history_provideraudithistory(update_kind_id, update_id);'" > create_index.log 2>&1 &
nohup bash -c "PGPASSWORD=$DATABASE_PASSWORD psql -U $DATABASE_USER -h $DATABASE_HOST -d $DATABASE_NAME -c \"SELECT * FROM bt_index_check('history_pro_update__9dff21_idx', true);\"" > verify_index.log 2>&1 &
