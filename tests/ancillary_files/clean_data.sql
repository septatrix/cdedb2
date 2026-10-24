-- Delete all data from all tables, except for the static postal code locations.
--
-- All tables are truncated by a single statement,
-- which is much faster than truncating them one by one.
-- Since all referencing tables are truncated as well,
-- CASCADE is not needed.
DO $$
    BEGIN
        EXECUTE (
            SELECT 'TRUNCATE ' || string_agg(format('%I.%I', table_schema, table_name), ', ')
            FROM information_schema.tables
            WHERE
                table_type = 'BASE TABLE'
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                AND table_name NOT IN ('postal_code_locations')
        );
    END;
$$;
