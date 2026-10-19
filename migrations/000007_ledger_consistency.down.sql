ALTER TABLE inbox_messages DROP CONSTRAINT ck_inbox_messages_transaction_id_not_nil;
ALTER TABLE outbox_events DROP CONSTRAINT ck_outbox_events_id_not_nil;
ALTER TABLE wallet_ledger_entries
    DROP CONSTRAINT ck_wallet_ledger_entries_transaction_id_not_nil,
    DROP CONSTRAINT ck_wallet_ledger_entries_wallet_id_not_nil,
    DROP CONSTRAINT ck_wallet_ledger_entries_id_not_nil;
ALTER TABLE wager_transactions
    DROP CONSTRAINT ck_wager_transactions_reference_transaction_id_not_nil,
    DROP CONSTRAINT ck_wager_transactions_player_id_not_nil,
    DROP CONSTRAINT ck_wager_transactions_wallet_id_not_nil,
    DROP CONSTRAINT ck_wager_transactions_id_not_nil;
ALTER TABLE wallets
    DROP CONSTRAINT ck_wallets_player_id_not_nil,
    DROP CONSTRAINT ck_wallets_id_not_nil;

DROP TRIGGER inbox_no_truncate ON inbox_messages;
DROP TRIGGER inbox_guard ON inbox_messages;
DROP TRIGGER processed_transaction_has_entry ON wager_transactions;
DROP TRIGGER ledger_entry_matches_transaction ON wallet_ledger_entries;

DROP FUNCTION inbox_guard();
DROP FUNCTION processed_transaction_has_entry();
DROP FUNCTION ledger_entry_matches_transaction();
