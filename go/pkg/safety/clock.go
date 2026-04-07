package safety

import "time"

// timeNow is a package-level indirection so tests can stub it. Production
// callers always get the wall clock.
var timeNow = time.Now
