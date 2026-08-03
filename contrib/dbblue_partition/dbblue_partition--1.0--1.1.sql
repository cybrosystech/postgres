/* contrib/dbblue_partition/dbblue_partition--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_partition UPDATE TO '1.1'" to load this file. \quit

/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_compat
 *
 * Make Odoo's schema introspection accept DBblue-partitioned tables
 * without modifying any Odoo source file.
 *
 * Odoo decides whether a model's table exists by reading relkind from the
 * *unqualified* name pg_class and only accepts 'r' — a partitioned table
 * ('p') is treated as missing, so a module update attempts CREATE TABLE
 * over it and aborts.  relkind cannot be faked in the real catalog: the
 * planner, pg_dump and pg_partman all branch on it, and a partitioned
 * table has no storage of its own.
 *
 * Instead, this function exploits the documented search_path rule that
 * pg_catalog is searched first ONLY when it is not listed explicitly: it
 * creates a schema dbblue_compat holding a pg_class view that reports
 * relkind 'r' for tables range-partitioned on a single "create_date"
 * column (the only shape dbblue_partition produces), and points the Odoo
 * role's search_path at it:
 *
 *		"$user", public, dbblue_compat, pg_catalog
 *
 * Consequences, by reader:
 *	- Odoo (unqualified pg_class):        sees 'r'  -> keeps working
 *	- planner / relcache:                 real catalog, untouched
 *	- pg_dump, psql \d:                   schema-qualified, see 'p'
 *	- pg_partman (search_path pinned to
 *	  its own schema + pg_catalog):       sees 'p'  -> keeps working
 *	- dbblue_partition itself:            pins/qualifies pg_catalog
 *
 * The view lives after "public" in the path, so current_schema and
 * unqualified CREATE TABLE still resolve to public, exactly as stock
 * Odoo expects.
 *
 * Usage (as the database owner or a superuser):
 *
 *		SELECT dbblue_partition_odoo_compat('odoo_role');
 *
 * then reconnect (restart Odoo).  Without an argument only the view is
 * (re)created and no role is changed.  A role can always set this up for
 * itself; configuring another role requires superuser.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_compat(p_role name DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	v_cols	text;
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	-- Build the column list dynamically so the view survives pg_class
	-- layout changes across PostgreSQL versions.  Only relkind is
	-- translated, and only for single-column range partitioning on
	-- "create_date" — the shape dbblue_partition_model() creates.
	SELECT string_agg(
		CASE WHEN a.attname = 'relkind' THEN
			'CASE WHEN c.relkind = ''p'' AND COALESCE(pt.ok, false) '
			'THEN ''r''::"char" ELSE c.relkind END AS relkind'
		ELSE 'c.' || quote_ident(a.attname) END,
		', ' ORDER BY a.attnum)
	INTO v_cols
	FROM pg_attribute a
	WHERE a.attrelid = 'pg_catalog.pg_class'::regclass
	  AND a.attnum > 0 AND NOT a.attisdropped;

	CREATE SCHEMA IF NOT EXISTS dbblue_compat;
	GRANT USAGE ON SCHEMA dbblue_compat TO PUBLIC;

	EXECUTE format(
		'CREATE OR REPLACE VIEW dbblue_compat.pg_class AS '
		'SELECT %s FROM pg_catalog.pg_class c '
		'LEFT JOIN LATERAL ('
		'    SELECT true AS ok '
		'    FROM pg_catalog.pg_partitioned_table pt '
		'    JOIN pg_catalog.pg_attribute pa '
		'      ON pa.attrelid = pt.partrelid AND pa.attnum = pt.partattrs[0] '
		'    WHERE pt.partrelid = c.oid '
		'      AND pt.partstrat = ''r'' '
		'      AND pt.partnatts = 1 '
		'      AND pa.attname = ''create_date'''
		') pt ON true', v_cols);

	EXECUTE 'GRANT SELECT ON dbblue_compat.pg_class TO PUBLIC';

	IF p_role IS NOT NULL THEN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role) THEN
			RAISE EXCEPTION 'role "%" does not exist', p_role;
		END IF;
		EXECUTE format(
			'ALTER ROLE %I IN DATABASE %I SET search_path = "$user", public, dbblue_compat, pg_catalog',
			p_role, current_database());
		RAISE NOTICE 'dbblue_partition: role "%" now sees DBblue-partitioned tables as regular tables in database "%"; reconnect (restart Odoo) to take effect',
			p_role, current_database();
	ELSE
		RAISE NOTICE 'dbblue_partition: compatibility view dbblue_compat.pg_class is in place; activate it per role with SELECT dbblue_partition_odoo_compat(''<odoo role>'')';
	END IF;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_compat(name) FROM PUBLIC;


/* ------------------------------------------------------------------------
 * dbblue_partition_odoo_compat_remove
 *
 * Undo dbblue_partition_odoo_compat(): reset the role's search_path (when
 * a role is given) and drop the compatibility view and schema.
 * ------------------------------------------------------------------------
 */
CREATE FUNCTION @extschema@.dbblue_partition_odoo_compat_remove(p_role name DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
	PERFORM @extschema@.dbblue_partition_enabled_check();

	IF p_role IS NOT NULL THEN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_role) THEN
			RAISE EXCEPTION 'role "%" does not exist', p_role;
		END IF;
		EXECUTE format('ALTER ROLE %I IN DATABASE %I RESET search_path',
					   p_role, current_database());
	END IF;

	DROP VIEW IF EXISTS dbblue_compat.pg_class;
	DROP SCHEMA IF EXISTS dbblue_compat;

	RAISE NOTICE 'dbblue_partition: Odoo compatibility view removed%',
		CASE WHEN p_role IS NOT NULL
			 THEN format('; search_path of role "%s" reset', p_role) ELSE '' END;
END
$$;

REVOKE ALL ON FUNCTION @extschema@.dbblue_partition_odoo_compat_remove(name) FROM PUBLIC;
