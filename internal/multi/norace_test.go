//go:build multi && !race

package multi

// raceFlags builds the two binaries the way this suite was built. Without the
// detector the build is a few seconds rather than a minute, which is what
// `go test -tags multi` without -race is asking for; `make test-multi` asks for
// the other one.
var raceFlags []string
