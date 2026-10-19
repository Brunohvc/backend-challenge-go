DROP TRIGGER outbox_guard ON outbox_events;
DROP TRIGGER ledger_matches_wallet ON wallet_ledger_entries;
DROP TRIGGER ledger_no_truncate ON wallet_ledger_entries;
DROP TRIGGER ledger_no_update ON wallet_ledger_entries;
DROP TRIGGER ledger_entry_check ON wallet_ledger_entries;
DROP TRIGGER wager_transactions_no_truncate ON wager_transactions;
DROP TRIGGER wager_transactions_guard ON wager_transactions;
DROP TRIGGER wallet_matches_ledger ON wallets;
DROP TRIGGER wallets_no_truncate ON wallets;
DROP TRIGGER wallets_guard ON wallets;

DROP FUNCTION outbox_guard();
DROP FUNCTION assert_wallet_matches_ledger();
DROP FUNCTION ledger_entry_check();
DROP FUNCTION ledger_append_only();
DROP FUNCTION wager_transactions_guard();
DROP FUNCTION wallets_guard();
