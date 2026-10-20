-- Delete all data from all tables, except for the static postal code locations.
--
-- For small amounts of data (like our sample data),
-- DELETE is much faster than TRUNCATE,
-- since the latter has to create new files for every table and index.
-- All tables are emptied by a single statement using data-modifying CTEs.
-- Foreign key constraints are only checked at the end of the statement,
-- when all tables are empty, so the order of deletion does not matter.
DO $$
    BEGIN
        EXECUTE (
            SELECT 'WITH ' || string_agg(format('d%s AS (DELETE FROM %I.%I)', i, table_schema, table_name), ', ') || ' SELECT'
            FROM (
                SELECT table_schema, table_name, row_number() OVER () AS i
                FROM information_schema.tables
                WHERE
                    table_type = 'BASE TABLE'
                    AND table_schema NOT IN ('pg_catalog', 'information_schema')
                    AND table_name NOT IN ('postal_code_locations')
            ) AS t
        );
    END;
$$;
