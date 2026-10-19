CREATE ROLE wallet_owner LOGIN PASSWORD 'wallet_owner';
CREATE ROLE wallet_app LOGIN PASSWORD 'wallet_app';

ALTER DATABASE wallet OWNER TO wallet_owner;
ALTER SCHEMA public OWNER TO wallet_owner;

REVOKE ALL ON DATABASE wallet FROM PUBLIC;
GRANT CONNECT ON DATABASE wallet TO wallet_owner, wallet_app;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO wallet_app;
