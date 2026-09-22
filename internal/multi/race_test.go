//go:build multi && race

package multi

// raceFlags builds the two binaries the way this suite was built. The suite is
// driving them as the system under test, so a data race inside one of them is a
// finding of this run rather than of some later one.
var raceFlags = []string{"-race"}
