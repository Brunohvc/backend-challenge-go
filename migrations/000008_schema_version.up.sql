CREATE FUNCTION public.schema_version() RETURNS TABLE (version bigint, dirty boolean)
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
AS $$
    SELECT m.version, m.dirty FROM public.schema_migrations AS m
$$;

REVOKE ALL ON FUNCTION public.schema_version() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.schema_version() TO wallet_app;
