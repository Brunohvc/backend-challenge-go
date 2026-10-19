//go:build !failpoints

package failpoint

// Inject does nothing: failpoints only exist in builds with -tags failpoints.
func Inject(string) {}
