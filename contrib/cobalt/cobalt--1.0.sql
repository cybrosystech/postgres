/* contrib/cobalt/cobalt--1.0.sql */

\echo Use "CREATE EXTENSION cobalt" to load this file. \quit

CREATE FUNCTION cobalt_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD cobalt TYPE INDEX HANDLER cobalt_handler;
COMMENT ON ACCESS METHOD cobalt IS 'cobalt index access method: bitmap-posting trigram index';

CREATE OPERATOR CLASS cobalt_text_ops
DEFAULT FOR TYPE text USING cobalt AS
	OPERATOR	1	~~ (text, text),
	OPERATOR	2	~~* (text, text);
