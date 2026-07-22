# CryptoSentinel Cleanup Plan

Run date: 2026-07-21 UTC
Baseline tests passing: 427
Rebase fallback: false

## Prior run (2026-05-31) completed items (already on branch)
- [x] Deleted 4 engine shims: risk_engine.py, lob_engine.py, order_manager.py, websocket_consumer.py
- [x] models.py: Optional[X] -> X | None
- [x] 7 except-Exception narrowings in db_writer.py, ws_consumer.py, lob_recorder.py, _logic.py, scorer.py

---

## F. Legacy Shims (engine/ directory)

- DEFERRED: engine/microstructure_engine.py -- only importer is tests/test_microstructure_engine.py (30 passing tests); deleting would drop baseline 427->~397, violating -3 threshold. Needs coordinated test rewrite first.

## A. Dead Code & Duplicates

No new items.

## E. Consolidation Opportunities

- DEFERRED: strategy/builder.py:200-203 and backtesting/metrics.py:238-245 -- Sharpe near-identical but edge-cases differ; consolidating risks subtle numeric drift.

## B. Type Safety Gaps

No new items.

## C. Error Handling Defects

- [x] engine/lob_snapshot_writer.py:85 -- added import sqlite3; except Exception -> except (sqlite3.Error, OSError)
- [x] strategy/executor.py:620 -- except Exception -> except RuntimeError
- [x] core/signal_telemetry.py:136 -- except Exception -> except (TypeError, RuntimeError)

## D. Test Hygiene Issues

- [x] tests/test_bt_tick_replay.py:22 -- removed ReplayTrade from import
- [x] tests/test_bt_vectorbt.py:19 -- removed OptimisationResult from import
- [x] tests/test_bt_walk_forward.py:22 -- removed WalkForwardWindow from import
- [x] tests/test_features.py:6 -- removed WelfordOnline from import
- [x] tests/test_executor.py:4 -- removed unused patch import
- [x] tests/test_executor.py:8 -- removed unused settings module-level import
- [x] tests/test_order_manager.py:10 -- removed call from import tuple
- [x] tests/test_registry.py:18 -- removed EntryRules and StatisticalValidity from import

## Risk Assessment

- engine/realtime_hub.py:46 -- except Exception in broadcast() intentionally broad. KEEP AS-IS.
- core/startup_reconciler.py and core/ws_consumer.py:197 -- intentional reconnect safety nets. KEEP AS-IS.
- execution/order_manager.py:180 -- silent pass on best-effort close_connection(). KEEP AS-IS.

## Deferred Items

- DEFERRED: engine/microstructure_engine.py -- see F above
- DEFERRED: Sharpe consolidation -- see E above

---

## Review Pass 1 -- 0 removed, 0 corrected, 0 added

All C items re-grepped and verified:
- C1 lob_snapshot_writer: db_writer.write_lob_snapshot uses sqlite3.connect (db_writer.py:184); hub.broadcast swallows per-ws errors internally at realtime_hub.py:46
- C3 executor: asyncio.create_task only raises RuntimeError on no-running-loop; TypeError impossible (coroutine type fixed at call site)
- C4 signal_telemetry: hub.broadcast propagates only TypeError (json.dumps) or RuntimeError; per-ws send errors caught inside broadcast()
All D items grepped: symbols appear only on import lines, not in test bodies
No __all__ entries, no @validator, no getattr string dispatch for any affected symbol
Risk items verified as intentional design decisions

## STATUS: APPROVED

---

## Reverted Changes

None

## Final Test Results

Baseline: 427 passing
Final: 427 passing
