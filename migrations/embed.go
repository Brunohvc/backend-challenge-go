// Package migrations embeds the versioned schema of the wallet database.
package migrations

import "embed"

// FS holds the migration files, named NNNNNN_name.up.sql and NNNNNN_name.down.sql.
//
//go:embed *.sql
var FS embed.FS
