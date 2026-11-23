--
-- DBblue fillfactor map.
--
-- A malformed map must be rejected at SET / reload time (the check hook),
-- instead of being accepted and then warned about -- twice -- on every
-- connection.  A valid map lowers the fillfactor of a matching CREATE TABLE.
-- Self-contained: dff_ objects.

-- (1) Malformed maps are refused at SET, each with a clear reason.
SET dbblue_fillfactor_map = 'dff.hot';              -- missing ":fillfactor"
SET dbblue_fillfactor_map = 'dff.hot:abc';          -- non-numeric fillfactor
SET dbblue_fillfactor_map = 'dff.hot:5';            -- below the [10,100] range
SET dbblue_fillfactor_map = 'dff.hot:150';          -- above the range
SET dbblue_fillfactor_map = ':70';                  -- empty table name
SET dbblue_fillfactor_map = 'dff.hot:70, broken';   -- a later entry is malformed
-- a 64-byte name: longer than any table name can be, so it could never match
SELECT set_config('dbblue_fillfactor_map', 'dff_' || repeat('x', 60) || ':70', false);

-- A failed SET leaves the previous value in place.
SHOW dbblue_fillfactor_map;

-- (2) A valid map is accepted with no error and no warning.
SET dbblue_fillfactor_map = 'dff.hot:70, dff.warm:75';
SHOW dbblue_fillfactor_map;

-- (3) A matching table gets the mapped fillfactor...
CREATE TABLE dff_hot (id int, v text);
SELECT reloptions FROM pg_class WHERE relname = 'dff_hot';

-- ...a non-matching table is left alone...
CREATE TABLE dff_cold (id int);
SELECT reloptions FROM pg_class WHERE relname = 'dff_cold';

-- ...and an explicit fillfactor on the statement wins over the map.
CREATE TABLE dff_warm (id int) WITH (fillfactor = 90);
SELECT reloptions FROM pg_class WHERE relname = 'dff_warm';

-- (4) An empty map is valid (feature off).
SET dbblue_fillfactor_map = '';
SHOW dbblue_fillfactor_map;

-- (5) A name of 63 bytes, the longest a table name can be, is accepted and
-- matches.  It also matches a longer name in CREATE TABLE, which PostgreSQL
-- truncates to those same 63 bytes.
SELECT set_config('dbblue_fillfactor_map', 'dff_' || repeat('x', 59) || ':65', false) IS NOT NULL AS accepted;
DO $$ BEGIN EXECUTE format('CREATE TABLE %I (id int)', 'dff_' || repeat('x', 59) || '_and_more'); END $$;
SELECT length(relname) AS name_bytes, reloptions FROM pg_class WHERE relname = 'dff_' || repeat('x', 59);
DO $$ BEGIN EXECUTE format('DROP TABLE %I', 'dff_' || repeat('x', 59)); END $$;

-- (6) dbblue_fillfactor_lookup() reports what the map gives a name, matching
-- the way CREATE TABLE does: model or table name, any case; NULL if none.
SET dbblue_fillfactor_map = 'dff.hot:70, dff.warm:75';
SELECT dbblue_fillfactor_lookup('dff.hot') AS model_name,
       dbblue_fillfactor_lookup('DFF_HOT') AS table_name,
       dbblue_fillfactor_lookup('dff_cold') AS unmapped;
SET dbblue_fillfactor_map = '';
SELECT dbblue_fillfactor_lookup('dff_hot') AS empty_map;

-- cleanup
RESET dbblue_fillfactor_map;
DROP TABLE dff_hot, dff_cold, dff_warm;
