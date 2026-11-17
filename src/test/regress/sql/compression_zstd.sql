-- Tests for TOAST compression with zstd

-- zstd is listed as a value of default_toast_compression on every build, so
-- probe whether it can actually be used: it can't on a build without
-- --with-zstd unless zstd_toast_compat is in shared_preload_libraries, nor
-- when dbblue_allow_zstd is off.
CREATE FUNCTION pg_temp.zstd_usable() RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  PERFORM set_config('default_toast_compression', 'zstd', true);
  RETURN true;
EXCEPTION WHEN feature_not_supported THEN
  RETURN false;
END $$;
SELECT NOT pg_temp.zstd_usable() AS skip_test \gset
\if :skip_test
   \echo '*** skipping TOAST tests with zstd (not supported) ***'
   \quit
\endif

CREATE SCHEMA zstd;
SET search_path TO zstd, public;

-- Ensure we get stable results regardless of the installation's default.
SET default_toast_compression = 'pglz';

-- values compressed inline (very compressible) and out of line (less so)
CREATE TABLE cmdata_zstd (id int, f1 text COMPRESSION zstd);
INSERT INTO cmdata_zstd VALUES (1, repeat('1234567890', 1000));
INSERT INTO cmdata_zstd
  SELECT 2, string_agg(md5(i::text), '') FROM generate_series(1, 2000) i;
SELECT attcompression FROM pg_attribute
  WHERE attrelid = 'cmdata_zstd'::regclass AND attname = 'f1';
SELECT id, pg_column_compression(f1), length(f1),
       pg_column_toast_chunk_id(f1) IS NOT NULL AS external
  FROM cmdata_zstd ORDER BY id;

-- values decompress to exactly what was stored
SELECT id, md5(f1) = CASE id
         WHEN 1 THEN md5(repeat('1234567890', 1000))
         ELSE (SELECT md5(string_agg(md5(i::text), '')) FROM generate_series(1, 2000) i)
       END AS intact
  FROM cmdata_zstd ORDER BY id;

-- slices
SELECT id, substr(f1, 1, 12), substr(f1, 9995, 6), length(left(f1, 100))
  FROM cmdata_zstd WHERE id = 1;
SELECT substr(f1, 63969, 32) = md5('2000') AS last_slice_ok
  FROM cmdata_zstd WHERE id = 2;

-- index lookups on zstd values
CREATE INDEX cmdata_zstd_idx ON cmdata_zstd (md5(f1));
SET enable_seqscan = off;
SELECT id FROM cmdata_zstd WHERE md5(f1) = md5(repeat('1234567890', 1000));
RESET enable_seqscan;

-- default_toast_compression = zstd applies to columns using the default
SET default_toast_compression = 'zstd';
CREATE TABLE cmdata_default (f1 text);
INSERT INTO cmdata_default VALUES (repeat('1234567890', 1000));
SELECT pg_column_compression(f1) FROM cmdata_default;

-- system catalogs are never compressed with zstd, whatever the default
DO $$ BEGIN
  EXECUTE format('COMMENT ON TABLE cmdata_default IS %L',
                 'catalog value ' || repeat('0123456789', 1000));
END $$;
SELECT pg_column_compression(description) <> 'zstd' AS catalog_not_zstd
  FROM pg_description WHERE objoid = 'cmdata_default'::regclass;
SET default_toast_compression = 'pglz';

-- changing the method affects new values only; existing ones keep zstd
ALTER TABLE cmdata_zstd ALTER COLUMN f1 SET COMPRESSION pglz;
INSERT INTO cmdata_zstd VALUES (3, repeat('1234567890', 1000));
SELECT id, pg_column_compression(f1) FROM cmdata_zstd ORDER BY id;
ALTER TABLE cmdata_zstd ALTER COLUMN f1 SET COMPRESSION zstd;

-- values are copied as they are, even into a column using another method
CREATE TABLE cmdata_copy (id int, f1 text COMPRESSION pglz);
INSERT INTO cmdata_copy SELECT * FROM cmdata_zstd;
SELECT id, pg_column_compression(f1) FROM cmdata_copy ORDER BY id;
-- ... unless the value is changed
INSERT INTO cmdata_copy SELECT id + 10, f1 || '' FROM cmdata_zstd WHERE id = 1;
SELECT pg_column_compression(f1) FROM cmdata_copy WHERE id = 11;

-- rewriting the column recompresses
ALTER TABLE cmdata_copy ALTER COLUMN f1 TYPE text USING f1 || '';
SELECT count(*) FILTER (WHERE pg_column_compression(f1) = 'zstd') AS zstd_left
  FROM cmdata_copy;

-- VACUUM FULL keeps zstd values
VACUUM FULL cmdata_zstd;
SELECT id, pg_column_compression(f1) FROM cmdata_zstd ORDER BY id;

-- the method is copied to partitions, inheritance children and LIKE copies
CREATE TABLE cmpart (d int, f1 text COMPRESSION zstd) PARTITION BY RANGE (d);
CREATE TABLE cmpart1 PARTITION OF cmpart FOR VALUES FROM (0) TO (10);
CREATE TABLE cmchild () INHERITS (cmdata_zstd);
CREATE TABLE cmlike (LIKE cmdata_zstd INCLUDING COMPRESSION);
CREATE TABLE cmlike_none (LIKE cmdata_zstd);
SELECT attrelid::regclass, attcompression FROM pg_attribute
  WHERE attname = 'f1'
    AND attrelid IN ('cmpart1'::regclass, 'cmchild'::regclass,
                     'cmlike'::regclass, 'cmlike_none'::regclass)
  ORDER BY attrelid::regclass::text;
INSERT INTO cmpart VALUES (1, repeat('1234567890', 1000));
SELECT pg_column_compression(f1) FROM cmpart;

-- zstd inside composite values and arrays survives a round trip
CREATE TYPE cmpair AS (a int, b text);
CREATE TABLE cmnested (p cmpair, arr text[]);
INSERT INTO cmnested
  SELECT ROW(1, f1)::cmpair, ARRAY[f1, 'x'] FROM cmdata_zstd WHERE id = 1;
SELECT md5((p).b) = md5(repeat('1234567890', 1000)) AS composite_ok,
       md5(arr[1]) = md5(repeat('1234567890', 1000)) AS array_ok
  FROM cmnested;

-- jsonb and bytea
CREATE TABLE cmtypes (j jsonb COMPRESSION zstd, b bytea COMPRESSION zstd);
INSERT INTO cmtypes
  SELECT jsonb_agg(i), convert_to(repeat('abcdefgh', 2000), 'UTF8')
  FROM generate_series(1, 2000) i;
SELECT pg_column_compression(j) AS j, pg_column_compression(b) AS b,
       jsonb_array_length(j), length(b)
  FROM cmtypes;

-- invalid uses
CREATE TABLE cmbad (f1 int COMPRESSION zstd);

SET client_min_messages = warning;
DROP SCHEMA zstd CASCADE;
RESET client_min_messages;
