// Package orchestrator is a stateless reshape of the Python
// CompactionOrchestrator: ProcessTable(ctx, table, priorState) returns
// (action, newState) without holding any in-memory state across calls.
// Persistence is delegated to pkg/state.
package orchestrator
