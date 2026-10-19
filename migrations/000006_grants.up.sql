GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, outbox_events, inbox_messages TO wallet_app;
GRANT SELECT, INSERT ON wallet_ledger_entries TO wallet_app;

REVOKE UPDATE, DELETE, TRUNCATE ON wallet_ledger_entries FROM wallet_app;
REVOKE DELETE, TRUNCATE ON wallets, wager_transactions, outbox_events, inbox_messages FROM wallet_app;
