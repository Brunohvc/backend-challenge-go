CREATE FUNCTION wallets_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'wallets cannot be truncated'
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallet % cannot be deleted', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at)
        IS DISTINCT FROM (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'wallet %: id, player, currency and creation time cannot change', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet %: a balance change must move the version from % to %, not to %',
            OLD.id, OLD.version, OLD.version + 1, NEW.version
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet %: the version only changes with the balance', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION wager_transactions_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'wager_transactions cannot be truncated'
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'transaction % cannot be deleted', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is %, a final status, and cannot change', OLD.id, OLD.status
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF (NEW.id, NEW.origin, NEW.kind, NEW.wallet_id, NEW.player_id, NEW.amount_minor, NEW.currency,
        NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
        NEW.round_id, NEW.game_id, NEW.reference_external_transaction_id, NEW.correlation_id, NEW.created_at)
        IS DISTINCT FROM
       (OLD.id, OLD.origin, OLD.kind, OLD.wallet_id, OLD.player_id, OLD.amount_minor, OLD.currency,
        OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
        OLD.round_id, OLD.game_id, OLD.reference_external_transaction_id, OLD.correlation_id, OLD.created_at) THEN
        RAISE EXCEPTION 'transaction %: only the status, the outcome and the schedule can change', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
        RAISE EXCEPTION 'transaction % cannot go back from PENDING_REFERENCE to PENDING', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF NEW.reference_attempts < OLD.reference_attempts OR NEW.permanent_failures < OLD.permanent_failures THEN
        RAISE EXCEPTION 'transaction %: attempt counters cannot go back', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING_REFERENCE'
        AND NEW.expires_at IS DISTINCT FROM OLD.expires_at THEN
        RAISE EXCEPTION 'transaction %: the expiry of the wait for a reference cannot move', OLD.id
            USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION ledger_append_only() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'WL002', CONSTRAINT = TG_NAME;
END
$$;

CREATE FUNCTION ledger_entry_check() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    w    record;
    tx   record;
    prev record;
BEGIN
    SELECT currency INTO w FROM wallets WHERE id = NEW.wallet_id FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;
    SELECT wallet_id, kind, amount_minor, currency INTO tx FROM wager_transactions WHERE id = NEW.transaction_id;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    IF NEW.currency <> w.currency THEN
        RAISE EXCEPTION 'ledger entry % is in % but wallet % is in %', NEW.id, NEW.currency, NEW.wallet_id, w.currency
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;
    IF (NEW.wallet_id, NEW.amount_minor, NEW.currency) IS DISTINCT FROM (tx.wallet_id, tx.amount_minor, tx.currency) THEN
        RAISE EXCEPTION 'ledger entry % moves % % on wallet % but transaction % is % % on wallet %',
            NEW.id, NEW.amount_minor, NEW.currency, NEW.wallet_id,
            NEW.transaction_id, tx.amount_minor, tx.currency, tx.wallet_id
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;
    IF (tx.kind = 'BET' AND NEW.direction <> 'DEBIT')
        OR (tx.kind IN ('OPENING', 'WIN', 'REFUND') AND NEW.direction <> 'CREDIT') THEN
        RAISE EXCEPTION 'ledger entry %: a % cannot be a %', NEW.id, tx.kind, NEW.direction
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;

    SELECT seq, balance_after_minor INTO prev
      FROM wallet_ledger_entries
     WHERE wallet_id = NEW.wallet_id
     ORDER BY seq DESC
     LIMIT 1;
    IF FOUND THEN
        IF NEW.seq <= prev.seq OR NEW.balance_before_minor <> prev.balance_after_minor THEN
            RAISE EXCEPTION 'ledger entry % (seq %, from %) does not continue wallet % (last seq %, balance %)',
                NEW.id, NEW.seq, NEW.balance_before_minor, NEW.wallet_id, prev.seq, prev.balance_after_minor
                USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
        END IF;
    ELSIF NEW.balance_before_minor <> 0 THEN
        RAISE EXCEPTION 'ledger entry % is the first of wallet % and must start from 0, not %',
            NEW.id, NEW.wallet_id, NEW.balance_before_minor
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION assert_wallet_matches_ledger() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    target uuid;
    w      bigint;
    l      bigint;
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN
        target := NEW.id;
    ELSE
        target := NEW.wallet_id;
    END IF;
    SELECT balance_minor INTO w FROM wallets WHERE id = target;
    SELECT balance_after_minor INTO l
      FROM wallet_ledger_entries
     WHERE wallet_id = target
     ORDER BY seq DESC
     LIMIT 1;
    IF w IS DISTINCT FROM COALESCE(l, 0) THEN
        RAISE EXCEPTION 'wallet % balance % does not match ledger %', target, w, COALESCE(l, 0)
            USING ERRCODE = 'WL001', CONSTRAINT = TG_NAME;
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION outbox_guard() RETURNS trigger
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

CREATE TRIGGER wallets_guard
    BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();
CREATE TRIGGER wallets_no_truncate
    BEFORE TRUNCATE ON wallets
    FOR EACH STATEMENT EXECUTE FUNCTION wallets_guard();
CREATE CONSTRAINT TRIGGER wallet_matches_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_wallet_matches_ledger();

CREATE TRIGGER wager_transactions_guard
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();
CREATE TRIGGER wager_transactions_no_truncate
    BEFORE TRUNCATE ON wager_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION wager_transactions_guard();

CREATE TRIGGER ledger_entry_check
    BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entry_check();
CREATE TRIGGER ledger_no_update
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER ledger_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();
CREATE CONSTRAINT TRIGGER ledger_matches_wallet
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_wallet_matches_ledger();

CREATE TRIGGER outbox_guard
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_guard();
