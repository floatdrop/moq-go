//go:build !unix

package conference

import "time"

// processCPU is not measured off Unix; the report shows zero cores.
func processCPU() time.Duration { return 0 }
