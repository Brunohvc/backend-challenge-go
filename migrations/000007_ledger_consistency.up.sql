CREATE FUNCTION ledger_entry_matches_transaction() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    tx       record;
    ref_kind text;
    expected text;
BEGIN
    SELECT status, kind, reference_transaction_id INTO tx FROM wager_transactions WHERE id = NEW.transaction_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF tx.status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry % records transaction %, which is % and not PROCESSED',
            NEW.id, NEW.transaction_id, tx.status
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;

    CASE tx.kind
        WHEN 'OPENING', 'WIN', 'REFUND' THEN
            expected := 'CREDIT';
        WHEN 'BET' THEN
            expected := 'DEBIT';
        WHEN 'ROLLBACK' THEN
            SELECT kind INTO ref_kind FROM wager_transactions WHERE id = tx.reference_transaction_id;
            expected := CASE ref_kind
                WHEN 'BET' THEN 'CREDIT'
                WHEN 'WIN' THEN 'DEBIT'
                WHEN 'REFUND' THEN 'DEBIT'
            END;
        ELSE
            NULL;
    END CASE;
    IF expected IS NULL OR NEW.direction <> expected THEN
        RAISE EXCEPTION 'ledger entry %: a % (of %) cannot be a %',
            NEW.id, tx.kind, COALESCE(ref_kind, 'nothing'), NEW.direction
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION processed_transaction_has_entry() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    tx      record;
    entries bigint;
BEGIN
    SELECT id, wallet_id, kind, status INTO tx FROM wager_transactions WHERE id = NEW.id;
    IF NOT FOUND OR tx.status <> 'PROCESSED' OR tx.kind = 'LOSS' THEN
        RETURN NULL;
    END IF;
    SELECT count(*) INTO entries
      FROM wallet_ledger_entries
     WHERE wallet_id = tx.wallet_id AND transaction_id = tx.id;
    IF entries <> 1 THEN
        RAISE EXCEPTION 'transaction % is a PROCESSED % with % ledger entries on wallet %, not 1',
            tx.id, tx.kind, entries, tx.wallet_id
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION inbox_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'inbox_messages cannot be truncated'
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'inbox message % of % cannot be deleted', OLD.message_id, OLD.consumer_name
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF (NEW.consumer_name, NEW.message_id, NEW.payload_hash, NEW.received_at)
        IS DISTINCT FROM (OLD.consumer_name, OLD.message_id, OLD.payload_hash, OLD.received_at) THEN
        RAISE EXCEPTION 'inbox message % of %: what was received cannot change', OLD.message_id, OLD.consumer_name
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF OLD.processed_at IS NOT NULL
        AND (NEW.processed_at, NEW.transaction_id, NEW.outcome)
            IS DISTINCT FROM (OLD.processed_at, OLD.transaction_id, OLD.outcome) THEN
        RAISE EXCEPTION 'inbox message % of %: the completion is written once', OLD.message_id, OLD.consumer_name
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NEW;
END
$$;

CREATE CONSTRAINT TRIGGER ledger_entry_matches_transaction
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_entry_matches_transaction();
CREATE CONSTRAINT TRIGGER processed_transaction_has_entry
    AFTER INSERT OR UPDATE OF status ON wager_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION processed_transaction_has_entry();
CREATE TRIGGER inbox_guard
    BEFORE UPDATE OR DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION inbox_guard();
CREATE TRIGGER inbox_no_truncate
    BEFORE TRUNCATE ON inbox_messages
    FOR EACH STATEMENT EXECUTE FUNCTION inbox_guard();

ALTER TABLE wallets
    ADD CONSTRAINT ck_wallets_id_not_nil CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    ADD CONSTRAINT ck_wallets_player_id_not_nil CHECK (player_id <> '00000000-0000-0000-0000-000000000000');
ALTER TABLE wager_transactions
    ADD CONSTRAINT ck_wager_transactions_id_not_nil CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    ADD CONSTRAINT ck_wager_transactions_wallet_id_not_nil CHECK (wallet_id <> '00000000-0000-0000-0000-000000000000'),
    ADD CONSTRAINT ck_wager_transactions_player_id_not_nil CHECK (player_id <> '00000000-0000-0000-0000-000000000000'),
    ADD CONSTRAINT ck_wager_transactions_reference_transaction_id_not_nil
        CHECK (reference_transaction_id <> '00000000-0000-0000-0000-000000000000');
ALTER TABLE wallet_ledger_entries
    ADD CONSTRAINT ck_wallet_ledger_entries_id_not_nil CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    ADD CONSTRAINT ck_wallet_ledger_entries_wallet_id_not_nil CHECK (wallet_id <> '00000000-0000-0000-0000-000000000000'),
    ADD CONSTRAINT ck_wallet_ledger_entries_transaction_id_not_nil
        CHECK (transaction_id <> '00000000-0000-0000-0000-000000000000');
ALTER TABLE outbox_events
    ADD CONSTRAINT ck_outbox_events_id_not_nil CHECK (id <> '00000000-0000-0000-0000-000000000000');
ALTER TABLE inbox_messages
    ADD CONSTRAINT ck_inbox_messages_transaction_id_not_nil
        CHECK (transaction_id <> '00000000-0000-0000-0000-000000000000');
