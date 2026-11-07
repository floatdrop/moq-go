//go:build !unix

package main

import "time"

// processCPU is not measured off Unix, and process_cpu_seconds_total is not
// served.
func processCPU() (time.Duration, bool) { return 0, false }
