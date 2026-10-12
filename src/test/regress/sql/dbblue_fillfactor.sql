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

-- cleanup
RESET dbblue_fillfactor_map;
DROP TABLE dff_hot, dff_cold, dff_warm;
