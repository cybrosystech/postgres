--
-- DBblue dedicated audit log: read-only-to-everyone grants.
--
-- A non-superuser pg_dump (the path Odoo's own backup uses) takes an ACCESS
-- SHARE lock on every table it dumps, which needs SELECT; once the dbblue
-- schema exists, a role without it makes the whole dump abort with "permission
-- denied for schema dbblue".  So the audit log is granted SELECT to PUBLIC
-- (plus USAGE on the schema and SELECT on its sequence), while every write
-- privilege stays revoked: the trail is readable by all -- so any role's
-- backup includes it -- but can never be forged, altered or erased.
--
-- This asserts that grant state after an audited write.  Self-contained:
-- dag_ / regress_dag_ objects plus a dbblue schema that is dropped again.

SET client_min_messages = warning;
\set VERBOSITY terse
SET dbblue_audit_operations = 'all';

CREATE ROLE regress_dag_reader;          -- an ordinary, unprivileged role
CREATE TABLE dag_t (id int PRIMARY KEY, v int);
SET dbblue_audit_tables = 'public.dag_t';
INSERT INTO dag_t VALUES (1, 10);        -- creates and grants the audit log

-- PUBLIC gets read, and nothing else.
SELECT has_schema_privilege('regress_dag_reader', 'dbblue', 'USAGE')                  AS schema_usage,
       has_table_privilege('regress_dag_reader', 'dbblue.dbblue_audit_log', 'SELECT') AS can_select,
       has_table_privilege('regress_dag_reader', 'dbblue.dbblue_audit_log', 'INSERT') AS can_insert,
       has_table_privilege('regress_dag_reader', 'dbblue.dbblue_audit_log', 'UPDATE') AS can_update,
       has_table_privilege('regress_dag_reader', 'dbblue.dbblue_audit_log', 'DELETE') AS can_delete,
       has_table_privilege('regress_dag_reader', 'dbblue.dbblue_audit_log', 'TRUNCATE') AS can_truncate;

-- The ordinary role can read the trail ...
SET ROLE regress_dag_reader;
SELECT count(*) >= 1 AS reader_sees_rows FROM dbblue.dbblue_audit_log;
-- ... but cannot modify it, advance its sequence, or create objects there.
INSERT INTO dbblue.dbblue_audit_log(rel_name, dml_op, changed_by, session_usr)
  VALUES ('x', 'INSERT', 'x', 'x');
SELECT nextval(pg_get_serial_sequence('dbblue.dbblue_audit_log', 'id'));
CREATE TABLE dbblue.nope (x int);
RESET ROLE;

-- cleanup
RESET dbblue_audit_tables;
RESET dbblue_audit_operations;
DROP SCHEMA dbblue CASCADE;
DROP TABLE dag_t;
DROP ROLE regress_dag_reader;
