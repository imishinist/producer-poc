#!/bin/bash

set -eo pipefail

id="$1"
name="$2"

usage() {
  echo "modify-members.sh <id> <name>" >&2
  exit 1
}

[[ -z "$id" || -z "$name" ]] && usage

psql -f <(cat <<EOF
begin;

UPDATE members SET name = '$name' WHERE id = $id;

INSERT INTO member_feed (member_id, last_txid, last_lsn, updated_at) VALUES
($id, pg_current_xact_id(), pg_current_wal_lsn(), now())
ON CONFLICT (member_id) DO UPDATE
SET last_txid = GREATEST(member_feed.last_txid, EXCLUDED.last_txid),
    last_lsn = GREATEST(member_feed.last_lsn, EXCLUDED.last_lsn),
    updated_at = GREATEST(member_feed.updated_at, EXCLUDED.updated_at);

SELECT * FROM members WHERE id = $id;
SELECT * FROM member_feed WHERE member_id = $id;

commit;
EOF
)