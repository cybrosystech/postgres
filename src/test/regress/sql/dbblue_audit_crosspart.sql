--
-- DBblue dedicated audit log: a cross-partition UPDATE is logged as one UPDATE.
--
-- Row movement (an UPDATE that changes which partition a row lives in) runs as
-- a DELETE from the old partition plus an INSERT into the new one.  Left alone
-- the delete half is suppressed and the insert half logs a bare INSERT with no
-- pre-image, so the change is misrecorded as a creation and the old values are
-- lost.  The fix suppresses the insert-half audit and records a single UPDATE
-- carrying both images, against the queried (root) table.
--
-- Self-contained: dac_ objects plus a dbblue schema that is dropped again.

SET client_min_messages = warning;      -- hide the one-time audit DDL notices
SET dbblue_audit_operations = 'all';

CREATE TABLE dac_part (id int, grp int, name text) PARTITION BY LIST (grp);
CREATE TABLE dac_part_a PARTITION OF dac_part FOR VALUES IN (1);
CREATE TABLE dac_part_b PARTITION OF dac_part FOR VALUES IN (2);
SET dbblue_audit_tables = 'public.dac_part, public.dac_part_a, public.dac_part_b';

INSERT INTO dac_part VALUES (1, 1, 'original');

-- Focus on the move alone.
TRUNCATE dbblue.dbblue_audit_log;

-- Cross-partition UPDATE: moves the row from dac_part_a (grp 1) to dac_part_b
-- (grp 2) and changes name.
UPDATE dac_part SET grp = 2, name = 'changed' WHERE id = 1;

-- Exactly one audit row: an UPDATE on the root table, with both images.
SELECT dml_op, rel_name,
       old_data->>'grp'  AS old_grp,  new_data->>'grp'  AS new_grp,
       old_data->>'name' AS old_name, new_data->>'name' AS new_name
  FROM dbblue.dbblue_audit_log ORDER BY id;

-- No stray INSERT or DELETE row for the move.
SELECT dml_op, count(*) FROM dbblue.dbblue_audit_log GROUP BY dml_op ORDER BY dml_op;

-- Same move, but with only the leaf partitions listed (not the root).  The move
-- is still audited -- as one UPDATE, recorded against the root table -- because
-- the source partition is tracked.
SET dbblue_audit_tables = 'public.dac_part_a, public.dac_part_b';
TRUNCATE dbblue.dbblue_audit_log;
UPDATE dac_part SET grp = 1, name = 'back' WHERE id = 1;   -- dac_part_b -> dac_part_a
SELECT dml_op, rel_name, old_data->>'grp' AS old_grp, new_data->>'grp' AS new_grp
  FROM dbblue.dbblue_audit_log ORDER BY id;

-- cleanup
RESET dbblue_audit_tables;
RESET dbblue_audit_operations;
DROP SCHEMA dbblue CASCADE;
DROP TABLE dac_part;
