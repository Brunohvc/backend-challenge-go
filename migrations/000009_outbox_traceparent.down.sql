CREATE OR REPLACE FUNCTION outbox_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF (NEW.id, NEW.seq, NEW.aggregate_type, NEW.aggregate_id, NEW.event_type, NEW.event_version, NEW.payload,
        NEW.correlation_id, NEW.causation_id, NEW.occurred_at, NEW.created_at)
        IS DISTINCT FROM
       (OLD.id, OLD.seq, OLD.aggregate_type, OLD.aggregate_id, OLD.event_type, OLD.event_version, OLD.payload,
        OLD.correlation_id, OLD.causation_id, OLD.occurred_at, OLD.created_at) THEN
        RAISE EXCEPTION 'outbox event %: the event cannot change once written', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % was published at % and stays so', OLD.id, OLD.published_at
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NEW;
END
$$;

ALTER TABLE outbox_events DROP COLUMN traceparent;
