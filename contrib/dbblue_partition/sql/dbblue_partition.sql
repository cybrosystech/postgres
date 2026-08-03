-- Tests for dbblue_partition.  Every scenario here corresponds to a defect
-- of the retired contrib/dbblue_partition C implementation or to a promise
-- in the extension's documentation.  All row dates are fixed so that error
-- messages naming leaf partitions stay stable; "today" tests only check
-- that statements succeed.

CREATE EXTENSION dbblue_partition CASCADE;
SET client_min_messages = warning;
-- Error CONTEXT pins PL/pgSQL line numbers and full procedure signatures,
-- which change on any edit to the extension scripts; the messages themselves
-- are what these tests assert.
\set SHOW_CONTEXT never
SET timezone = 'UTC';

-- Refuses to run until explicitly enabled (off by default)
CALL dbblue_partition_model('anything', p_odoo_compat => false);
SET dbblue_partition.enabled = on;

-- ---------------------------------------------------------------------
-- Validation: fail early, before touching anything
-- ---------------------------------------------------------------------
CALL dbblue_partition_model('no_such_table', p_odoo_compat => false);

CREATE TABLE val_t (id serial PRIMARY KEY, v text, create_date timestamp);
CALL dbblue_partition_model('val_t', 'no_such_column', p_odoo_compat => false);
CALL dbblue_partition_model('val_t', 'v', p_odoo_compat => false);				-- wrong type
INSERT INTO val_t (v, create_date) VALUES ('x', NULL);
CALL dbblue_partition_model('val_t', p_odoo_compat => false);					-- NULL in control column
DELETE FROM val_t;

CREATE TABLE val_nopk (create_date timestamp NOT NULL);
CALL dbblue_partition_model('val_nopk', p_odoo_compat => false);				-- no primary key

CREATE TABLE val_interval (id serial PRIMARY KEY, create_date timestamp NOT NULL);
CALL dbblue_partition_model('val_interval', 'create_date', '-1 month', p_odoo_compat => false);

-- ---------------------------------------------------------------------
-- The full Odoo-shaped scenario
-- ---------------------------------------------------------------------
CREATE TABLE res_partner (
    id serial PRIMARY KEY,
    name text UNIQUE,
    create_date timestamp DEFAULT now()
);

CREATE TABLE sale_order (
    id serial PRIMARY KEY,
    name text NOT NULL,
    partner_id integer REFERENCES res_partner(id) ON DELETE RESTRICT,
    company_id integer NOT NULL DEFAULT 1,
    amount numeric CHECK (amount >= 0),
    amount_tax numeric GENERATED ALWAYS AS (amount * 0.1) STORED,
    note text,
    create_date timestamp NOT NULL DEFAULT now(),
    CONSTRAINT sale_order_name_uniq UNIQUE (name),
    CONSTRAINT sale_order_id_company_uniq UNIQUE (id, company_id)
);
CREATE INDEX sale_order_partner_idx ON sale_order (partner_id);
CREATE INDEX sale_order_note_idx ON sale_order (note) WHERE note IS NOT NULL;

CREATE TABLE sale_order_line (
    id serial PRIMARY KEY,
    order_id integer REFERENCES sale_order(id) ON DELETE CASCADE,
    company_id integer,
    qty int,
    CONSTRAINT sol_order_company_fk FOREIGN KEY (order_id, company_id)
        REFERENCES sale_order (id, company_id)
);

CREATE VIEW sale_summary AS
    SELECT partner_id, sum(amount) AS total FROM sale_order GROUP BY partner_id;
CREATE VIEW sale_summary_top AS
    SELECT * FROM sale_summary WHERE total > 100;

CREATE FUNCTION touch_note() RETURNS trigger LANGUAGE plpgsql AS
$$ BEGIN NEW.note := coalesce(NEW.note, 'via-trigger'); RETURN NEW; END $$;
CREATE TRIGGER sale_order_touch BEFORE INSERT ON sale_order
    FOR EACH ROW EXECUTE FUNCTION touch_note();

INSERT INTO res_partner (name, create_date)
    SELECT 'partner ' || g, timestamp '2025-01-01' + (g || ' hours')::interval
    FROM generate_series(1, 50) g;
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    SELECT 'SO' || g, (g % 50) + 1, 1, g % 500,
           timestamp '2025-01-01' + (g || ' minutes')::interval
    FROM generate_series(1, 5000) g;
-- an old outlier, leaving a >1 year hole before the 2025 data
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO-old', 1, 1, 10, timestamp '2023-06-15 12:00:00');
INSERT INTO sale_order_line (order_id, company_id, qty)
    SELECT g, 1, g % 5 FROM generate_series(1, 5000) g;

CALL dbblue_partition_model('sale.order', 'create_date', '1 month', p_odoo_compat => false);

-- now partitioned, all rows retained, nothing stranded
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'sale_order';
SELECT count(*) FROM sale_order;
SELECT state, rows_at_conversion, rows_moved, partition_count > 20 AS gapless,
       default_partition_rows, backup_exists, backup_rows_remaining
FROM dbblue_partition_status('sale.order');

-- writable immediately: today (defect: current month never created) ...
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO-today', 2, 1, 42, now());
-- ... and inside the historical gap (defect: coverage holes)
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO-gap', 2, 1, 42, timestamp '2024-03-03');

-- CHECK constraints survive (defect: silently dropped)
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO-neg', 2, 1, -999, timestamp '2025-01-15');

-- UNIQUE survives, per partition (defect: silently dropped)
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO7', 2, 1, 1, timestamp '2025-01-16');

-- generated columns still compute (defect: property lost)
SELECT amount_tax FROM sale_order WHERE name = 'SO-gap';

-- triggers survive (defect: not carried over)
SELECT note FROM sale_order WHERE name = 'SO-gap';

-- secondary indexes survive on the parent (defect: all dropped)
SELECT indexname FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'sale_order'
ORDER BY indexname;

-- single-column incoming FK still enforced (fixed dates in parent rows)
INSERT INTO sale_order_line (order_id, company_id, qty) VALUES (999999, 1, 1);
-- multi-column incoming FK kept composite (defect: decomposed per column)
INSERT INTO sale_order_line (order_id, company_id, qty) VALUES (5, 12345, 1);
INSERT INTO sale_order_line (order_id, company_id, qty) VALUES (5, 1, 1);

-- outgoing FK still enforced
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO-badpartner', 999999, 1, 5, timestamp '2025-01-17');

-- ON DELETE CASCADE flows through the re-pointed FK
DELETE FROM sale_order WHERE name = 'SO5';
SELECT count(*) FROM sale_order_line WHERE order_id = 5;

-- dependent views recreated and functional (recursively)
SELECT count(*) > 0 FROM sale_summary;
SELECT count(*) >= 0 FROM sale_summary_top;

-- serial sequence kept and re-owned: ids continue, no restart
INSERT INTO sale_order (name, partner_id, company_id, amount, create_date)
    VALUES ('SO-seq', 3, 1, 5, timestamp '2025-01-18')
    RETURNING id > 5000 AS id_continues;

-- Odoo's unconditional DROP NOT NULL on the partition key: WARNING + no-op
ALTER TABLE sale_order ALTER COLUMN create_date DROP NOT NULL;
SELECT attnotnull FROM pg_attribute
WHERE attrelid = 'sale_order'::regclass AND attname = 'create_date';

-- backup guarded, then dropped
SELECT dbblue_partition_drop_backup('sale.order');
SELECT backup_exists FROM dbblue_partition_status('sale.order');
CALL dbblue_partition_model('sale.order', p_odoo_compat => false);

-- ---------------------------------------------------------------------
-- Self-referencing FK (account_move.reversed_entry_id pattern): the FK
-- constraint also carries conindid/conrelid of the source table, which
-- must not duplicate the index capture nor be dropped from the wrong
-- table during the swap.
-- ---------------------------------------------------------------------
CREATE TABLE acc_move (
    id serial PRIMARY KEY,
    reversed_entry_id int REFERENCES acc_move(id) ON DELETE SET NULL,
    create_date timestamp NOT NULL
);
INSERT INTO acc_move (reversed_entry_id, create_date)
    SELECT NULL, timestamp '2025-04-01' + (g || ' hours')::interval
    FROM generate_series(1, 100) g;
UPDATE acc_move SET reversed_entry_id = id - 1 WHERE id % 10 = 0;
CALL dbblue_partition_model('acc_move', 'create_date', '1 month', p_odoo_compat => false);
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'acc_move';
SELECT count(*) FROM acc_move;
INSERT INTO acc_move (reversed_entry_id, create_date)
    VALUES (999999, timestamp '2025-04-02');		-- self-FK enforced
DELETE FROM acc_move WHERE id = 9;					-- ON DELETE SET NULL flows
SELECT reversed_entry_id IS NULL AS set_null_ok FROM acc_move WHERE id = 10;
SELECT convalidated FROM pg_constraint
WHERE conname = 'acc_move_reversed_entry_id_fkey' AND conrelid = 'acc_move'::regclass;

-- ---------------------------------------------------------------------
-- Empty table: zero partitions was an unwritable-table defect
-- ---------------------------------------------------------------------
CREATE TABLE empty_model (id serial PRIMARY KEY, create_date timestamp NOT NULL DEFAULT now());
CALL dbblue_partition_model('empty_model', p_odoo_compat => false);
INSERT INTO empty_model (create_date) VALUES (now());
INSERT INTO empty_model (create_date) VALUES (timestamp '1999-01-01');
SELECT count(*) FROM empty_model;
SELECT default_partition_rows FROM dbblue_partition_status('empty_model');

-- ---------------------------------------------------------------------
-- Mixed-case, space-bearing identifiers and identity columns
-- (defect: %s interpolation broke quoted identifiers, invited injection,
--  and sequence handling assumed <table>_id_seq)
-- ---------------------------------------------------------------------
CREATE TABLE "Part Case" ("Id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                          "When Created" timestamptz NOT NULL);
INSERT INTO "Part Case" ("When Created")
    SELECT timestamptz '2025-02-01' + (g || ' days')::interval
    FROM generate_series(1, 100) g;
CALL dbblue_partition_model('Part Case', 'When Created', '1 month', p_odoo_compat => false);
SELECT count(*) FROM "Part Case";
INSERT INTO "Part Case" ("When Created") VALUES (now());
SELECT max("Id") = 101 AS identity_continued FROM "Part Case";

-- ---------------------------------------------------------------------
-- Undo restores the original table wholesale
-- ---------------------------------------------------------------------
CREATE TABLE undo_me (id serial PRIMARY KEY, v text UNIQUE, create_date timestamp NOT NULL);
CREATE TABLE undo_child (id serial PRIMARY KEY,
                         undo_id int REFERENCES undo_me(id) ON DELETE CASCADE);
CREATE VIEW undo_view AS SELECT count(*) AS n FROM undo_me;
INSERT INTO undo_me (v, create_date)
    SELECT 'v' || g, timestamp '2025-03-01' + (g || ' hours')::interval
    FROM generate_series(1, 1000) g;
INSERT INTO undo_child (undo_id) SELECT g FROM generate_series(1, 1000) g;

CALL dbblue_partition_model('undo_me', 'create_date', '1 month', p_odoo_compat => false);
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'undo_me';
CALL dbblue_partition_undo('undo_me');
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'undo_me';
SELECT count(*) FROM undo_me;
SELECT n FROM undo_view;
INSERT INTO undo_child (undo_id) VALUES (999999);		-- FK enforced again
INSERT INTO undo_me (v, create_date) VALUES ('v5', timestamp '2025-04-01');	-- global unique again
SELECT count(*) FROM dbblue_partition_status('undo_me');
SELECT count(*) FROM part_config WHERE parent_table = 'public.undo_me';
SELECT indexname FROM pg_indexes WHERE tablename = 'undo_me' ORDER BY indexname;

-- ---------------------------------------------------------------------
-- Publications: membership must move from the renamed backup to the new
-- parent on convert (else subscribers see the batched deletes), and undo
-- must restore membership without double-adding.
-- ---------------------------------------------------------------------
CREATE TABLE pub_t (id serial PRIMARY KEY, create_date timestamp NOT NULL);
INSERT INTO pub_t (create_date) VALUES (timestamp '2025-06-01');
CREATE PUBLICATION dbblue_test_pub FOR TABLE pub_t;
CALL dbblue_partition_model('pub_t', p_odoo_compat => false);
SELECT pr.prrelid::regclass::text AS published
FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid
WHERE p.pubname = 'dbblue_test_pub';
CALL dbblue_partition_undo('pub_t');
SELECT pr.prrelid::regclass::text AS published
FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid
WHERE p.pubname = 'dbblue_test_pub';
DROP PUBLICATION dbblue_test_pub;

-- ---------------------------------------------------------------------
-- A NOT VALID incoming FK (tolerating pre-existing violations) must stay
-- NOT VALID through both conversion and undo.
-- ---------------------------------------------------------------------
CREATE TABLE nv_parent (id serial PRIMARY KEY, create_date timestamp NOT NULL);
CREATE TABLE nv_child (id serial PRIMARY KEY, parent_id int);
INSERT INTO nv_parent (create_date) VALUES (timestamp '2025-06-01');
INSERT INTO nv_child (parent_id) VALUES (424242);	-- pre-existing violation
ALTER TABLE nv_child ADD CONSTRAINT nv_child_fk
    FOREIGN KEY (parent_id) REFERENCES nv_parent(id) NOT VALID;
CALL dbblue_partition_model('nv_parent', p_odoo_compat => false);
SELECT convalidated FROM pg_constraint WHERE conname = 'nv_child_fk';
CALL dbblue_partition_undo('nv_parent');
SELECT convalidated FROM pg_constraint WHERE conname = 'nv_child_fk';

-- ---------------------------------------------------------------------
-- Single-transaction mode is atomic: a ROLLBACK undoes the conversion
-- ---------------------------------------------------------------------
CREATE TABLE atomic_t (id serial PRIMARY KEY, create_date timestamp NOT NULL);
INSERT INTO atomic_t (create_date)
    SELECT timestamp '2025-05-01' + g * interval '1 hour' FROM generate_series(1, 500) g;
BEGIN;
CALL dbblue_partition_model('atomic_t', p_single_transaction => true, p_odoo_compat => false);
ROLLBACK;
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'atomic_t';
SELECT count(*) FROM atomic_t;
BEGIN;
CALL dbblue_partition_model('atomic_t', p_single_transaction => true, p_odoo_compat => false);
COMMIT;
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'atomic_t';
SELECT count(*) FROM atomic_t;

-- ---------------------------------------------------------------------
-- Odoo compatibility view: a role whose search_path lists pg_catalog
-- explicitly (after dbblue_compat) sees relkind 'r' for tables
-- partitioned by create_date, while the real catalog stays truthful.
-- This is what lets an unmodified Odoo run module updates against a
-- partitioned model table.
-- ---------------------------------------------------------------------
SELECT dbblue_partition_odoo_compat();
SET search_path = "$user", public, dbblue_compat, pg_catalog;
-- Odoo's view of the world: partitioned-by-create_date looks regular
SELECT relkind::text FROM pg_class WHERE relname = 'atomic_t';
-- the truth is unchanged
SELECT relkind::text FROM pg_catalog.pg_class WHERE relname = 'atomic_t';
-- a table partitioned by anything else is NOT masked
CREATE TABLE other_part (id int, d timestamp NOT NULL, PRIMARY KEY (id, d))
    PARTITION BY RANGE (d);
SELECT relkind::text FROM pg_class WHERE relname = 'other_part';
-- the planner still sees the real partitioned table through the view path
SELECT count(*) FROM atomic_t;
RESET search_path;
SELECT dbblue_partition_odoo_compat_remove();
SELECT count(*) FROM pg_namespace WHERE nspname = 'dbblue_compat';

-- ---------------------------------------------------------------------
-- 1.4 behaviour: a precision-qualified control column is accepted, the
-- default interval is 1 year, conversions are atomic by default, and a
-- dependent materialized view is carried across (recreated with its
-- index and refreshed only after the data has landed, so it holds the
-- same rows as before rather than an empty snapshot).
-- ---------------------------------------------------------------------
CREATE TABLE mv_t (id serial PRIMARY KEY, create_date timestamp(3) NOT NULL);
INSERT INTO mv_t (create_date)
    SELECT timestamp '2021-06-01' + (g || ' days')::interval
    FROM generate_series(1, 400) g;
CREATE MATERIALIZED VIEW mv_t_agg AS
    SELECT date_trunc('year', create_date) AS yr, count(*) AS n FROM mv_t GROUP BY 1;
CREATE INDEX mv_t_agg_yr ON mv_t_agg (yr);
COMMENT ON MATERIALIZED VIEW mv_t_agg IS 'per year';
CREATE MATERIALIZED VIEW mv_t_empty AS SELECT 1 AS x FROM mv_t WITH NO DATA;

CALL dbblue_partition_model('mv_t', p_odoo_compat => false);
SELECT partition_interval FROM part_config WHERE parent_table = 'public.mv_t';
SELECT count(*) FROM mv_t;
SELECT yr, n FROM mv_t_agg ORDER BY yr;                 -- must match pre-conversion
SELECT relispopulated FROM pg_catalog.pg_class WHERE relname = 'mv_t_agg';
SELECT relispopulated FROM pg_catalog.pg_class WHERE relname = 'mv_t_empty';
SELECT indexname FROM pg_indexes WHERE tablename = 'mv_t_agg';
SELECT obj_description('mv_t_agg'::regclass, 'pg_class');
-- status reports the partition set's real size, not the parent's zero
SELECT state, total_size <> '0 bytes' AS size_reported, rows_not_visible_to_odoo
FROM dbblue_partition_status('mv_t');

-- ---------------------------------------------------------------------
-- 1.4 behaviour: undo reconciles column drift by itself when the backup
-- is empty, so a module update between conversion and undo is not an
-- obstacle; added values survive and generated columns recompute.
-- ---------------------------------------------------------------------
ALTER TABLE mv_t ADD COLUMN note text;
ALTER TABLE mv_t ADD COLUMN doubled numeric GENERATED ALWAYS AS (id * 2) STORED;
UPDATE mv_t SET note = 'kept' WHERE id <= 3;
CALL dbblue_partition_undo('mv_t');
SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'mv_t';
SELECT count(*) AS rows, count(*) FILTER (WHERE note = 'kept') AS kept FROM mv_t;
SELECT doubled FROM mv_t WHERE id = 3;
SELECT count(*) FROM mv_t_agg;                          -- matview survived the undo
DROP MATERIALIZED VIEW mv_t_agg, mv_t_empty;
DROP TABLE mv_t;

-- ---------------------------------------------------------------------
-- Leave nothing behind: no dangling role search_path, no template
-- tables, no catalog or pg_partman rows.  Without this, a second
-- installcheck run fails on "backup table already exists".
-- ---------------------------------------------------------------------
SELECT dbblue_partition_odoo_compat_remove(current_user);
SELECT dbblue_partition_odoo_compat_remove(
           (SELECT pg_get_userbyid(datdba) FROM pg_database
            WHERE datname = current_database()));
SELECT count(*) AS roles_still_pointing_at_compat
FROM pg_db_role_setting s
JOIN pg_database d ON d.oid = s.setdatabase
WHERE d.datname = current_database()
  AND array_to_string(s.setconfig, ' ') LIKE '%dbblue_compat%';
\set SHOW_CONTEXT errors
