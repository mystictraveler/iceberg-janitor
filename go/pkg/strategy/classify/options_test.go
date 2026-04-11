package classify

import (
	"testing"
	"time"
)

// TestClassToOptions_ModePerClass verifies each workload class maps to
// the expected maintenance mode. This is the contract the server's
// /maintain handler relies on.
func TestClassToOptions_ModePerClass(t *testing.T) {
	cases := []struct {
		class WorkloadClass
		want  MaintainMode
	}{
		{ClassStreaming, ModeHot},
		{ClassBatch, ModeCold},
		{ClassSlowChanging, ModeCold},
		{ClassDormant, ModeCold},
	}
	for _, c := range cases {
		opts := ClassToOptions(c.class)
		if opts.Mode != c.want {
			t.Errorf("class %s: got mode %s, want %s", c.class, opts.Mode, c.want)
		}
	}
}

// TestClassToOptions_ReasonableThresholds verifies every class returns
// sensible (non-zero, monotonically-sized) thresholds.
func TestClassToOptions_ReasonableThresholds(t *testing.T) {
	all := []WorkloadClass{ClassStreaming, ClassBatch, ClassSlowChanging, ClassDormant}
	for _, class := range all {
		opts := ClassToOptions(class)
		if opts.KeepLastSnapshots <= 0 {
			t.Errorf("class %s: KeepLastSnapshots must be > 0, got %d", class, opts.KeepLastSnapshots)
		}
		if opts.KeepWithin <= 0 {
			t.Errorf("class %s: KeepWithin must be > 0, got %v", class, opts.KeepWithin)
		}
		if opts.TargetFileSizeBytes <= 0 {
			t.Errorf("class %s: TargetFileSizeBytes must be > 0, got %d", class, opts.TargetFileSizeBytes)
		}
		if opts.SmallFileThreshold <= 0 {
			t.Errorf("class %s: SmallFileThreshold must be > 0, got %d", class, opts.SmallFileThreshold)
		}
		if opts.StaleRewriteAge <= 0 {
			t.Errorf("class %s: StaleRewriteAge must be > 0, got %v", class, opts.StaleRewriteAge)
		}
	}
}

// TestClassToOptions_MonotonicRetention verifies retention windows
// grow from streaming to dormant (streaming has the shortest KeepWithin,
// dormant the longest).
func TestClassToOptions_MonotonicRetention(t *testing.T) {
	streaming := ClassToOptions(ClassStreaming).KeepWithin
	batch := ClassToOptions(ClassBatch).KeepWithin
	slow := ClassToOptions(ClassSlowChanging).KeepWithin
	dormant := ClassToOptions(ClassDormant).KeepWithin
	if !(streaming <= batch && batch <= slow && slow <= dormant) {
		t.Errorf("retention not monotonic: streaming=%v batch=%v slow=%v dormant=%v",
			streaming, batch, slow, dormant)
	}
}

// TestClassToOptions_MonotonicTargetFileSize verifies target file size
// also grows from streaming to dormant (streaming has smaller targets,
// dormant bigger).
func TestClassToOptions_MonotonicTargetFileSize(t *testing.T) {
	streaming := ClassToOptions(ClassStreaming).TargetFileSizeBytes
	batch := ClassToOptions(ClassBatch).TargetFileSizeBytes
	slow := ClassToOptions(ClassSlowChanging).TargetFileSizeBytes
	dormant := ClassToOptions(ClassDormant).TargetFileSizeBytes
	if !(streaming <= batch && batch <= slow && slow <= dormant) {
		t.Errorf("target file size not monotonic: streaming=%d batch=%d slow=%d dormant=%d",
			streaming, batch, slow, dormant)
	}
}

// TestClassToOptions_UnknownClass verifies an unknown class gets a
// conservative cold plan rather than a zero value.
func TestClassToOptions_UnknownClass(t *testing.T) {
	opts := ClassToOptions(WorkloadClass("garbage"))
	if opts.Mode != ModeCold {
		t.Errorf("unknown class: expected Mode=%s, got %s", ModeCold, opts.Mode)
	}
	if opts.KeepLastSnapshots <= 0 || opts.KeepWithin <= 0 {
		t.Error("unknown class: expected non-zero fallback thresholds")
	}
}

// TestClassToOptions_StreamingWriteBuffer verifies the streaming class
// carries the WriteBufferSeconds from Thresholds (decision #31).
func TestClassToOptions_StreamingWriteBuffer(t *testing.T) {
	opts := ClassToOptions(ClassStreaming)
	if opts.WriteBufferSeconds == 0 {
		t.Errorf("streaming class: expected non-zero WriteBufferSeconds, got 0")
	}
	// Thresholds default for streaming is 60s.
	if time.Duration(opts.WriteBufferSeconds)*time.Second != 60*time.Second {
		t.Errorf("streaming WriteBufferSeconds: got %d, want 60", opts.WriteBufferSeconds)
	}
}
