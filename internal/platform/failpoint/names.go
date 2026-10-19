// Package failpoint lets end-to-end tests crash the service at chosen critical points.
package failpoint

// The failpoints the end-to-end tests arm by name.
const (
	SubmitBeforeCommit              = "submit.before_commit"
	SubmitAfterCommit               = "submit.after_commit"
	ConsumerAfterCommitBeforeDelete = "consumer.after_commit_before_delete"
	OutboxAfterClaim                = "outbox.after_claim"
	OutboxAfterPublishBeforeAck     = "outbox.after_publish_before_ack"
	ResolverAfterClaim              = "resolver.after_claim"
)
