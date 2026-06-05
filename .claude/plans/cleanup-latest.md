# CryptoSentinel Cleanup Plan

Run date: 2026-05-31 UTC
Baseline tests passing: 443
Rebase fallback: false

---

## F. Legacy Shims (engine/ directory)

- [x] `engine/risk_engine.py` — pure re-export shim (`from risk.engine import *`); callers found by grep: none; action: delete file
- [x] `engine/lob_engine.py` — pure re-export shim (`from core.lob_engine import *`); callers found by grep: none; action: delete file
- [x] `engine/order_manager.py` — pure re-export shim (`from execution.order_manager import *`); callers found by grep: none; action: delete file
- [x] `engine/websocket_consumer.py` — pure re-export shim (`from core.ws_consumer import *`); callers found by grep: none; action: delete file
- NOTE: engine/microstructure_engine.py import fixed (`from .lob_engine` → `from core.lob_engine`) as collateral fix from shim deletion

## A. Dead Code & Duplicates

No items — production dead code beyond the engine shims above was not found after grepping all symbol references.

## E. Consolidation Opportunities

No items — LOB sync pattern in ws_consumer/_lob_recorder and wall detection logic are similar but diverge in interface and error recovery behaviour; consolidation would risk regressions.

## B. Type Safety Gaps

- [x] `models.py:5,181,182,197` — `Optional[WallState]` and `Optional[float]` are legacy typing-module forms; Python 3.11 supports the `X | None` union syntax natively; proposed: replace `Optional[WallState]` → `WallState | None` (lines 181–182), `Optional[float]` → `float | None` (line 197), then remove the now-unused `from typing import Optional` import (line 5)

## C. Error Handling Defects

- [x] `engine/db_writer.py:101` (`_candle_loop`) — `except Exception as e` wraps `asyncio.to_thread(self._write_candle, …)`; the worker calls `sqlite3.connect()` so the only expected failure type is `sqlite3.Error`; proposed: `except sqlite3.Error as e`
- [x] `engine/db_writer.py:109` (`_ms_bar_loop`) — same pattern as C1; proposed: `except sqlite3.Error as e`
- [x] `engine/db_writer.py:117` (`_portfolio_loop`) — same pattern as C1; proposed: `except sqlite3.Error as e`
- [x] `engine/db_writer.py:125` (`_cleanup_loop`) — same pattern as C1; proposed: `except sqlite3.Error as e`
- [x] `core/ws_consumer.py:299` (`_sync_lob_snapshot`) — `except Exception as exc` wraps `aiohttp.ClientSession().get()`, `resp.raise_for_status()`, `resp.json()`, and dict/float parsing; all expected failure types: `aiohttp.ClientError` (includes `ClientResponseError`), `asyncio.TimeoutError`, `json.JSONDecodeError`, `KeyError`, `ValueError`; proposed: `except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc`
- [x] `core/lob_recorder.py:184` (`_sync_snapshot`) — identical aiohttp REST pattern to C5; proposed: `except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc`
- [x] `dashboard/_logic.py:15` (`fire_killswitch`) — `except Exception` wraps `requests.post(…)`; library raises `requests.RequestException`; also fixed `tests/test_dashboard_live.py:102` mock (was using builtin `ConnectionError`, now uses `requests.RequestException`)
- [x] `strategy/scorer.py:184` (`ScorerFactory.load_or_fallback`) — `except Exception as exc` wraps `joblib.load(path)` + model-quality checks; expected failures: `OSError`, `EOFError`, `KeyError`, `ValueError`, `RuntimeError`; proposed: `except (OSError, EOFError, KeyError, ValueError, RuntimeError) as exc`

## D. Test Hygiene Issues

No actionable items — test_microstructure_engine.py tests a live module and removing it would breach the −3 regression threshold (see Deferred).

---

## Risk Assessment

- `engine/realtime_hub.py:46` — `except Exception` in `broadcast()` is intentionally broad: we want to catch every possible send failure to drop dead WebSocket connections without leaving stale handles; narrowing to specific aiohttp types would miss `RuntimeError`/`ConnectionResetError` variants. KEEP AS-IS.
- `core/startup_reconciler.py:59,79,89,110,176` — broad `except Exception` blocks are deliberate per design ("never raises"); tightening would undermine the safety guarantee. KEEP AS-IS.
- `core/ws_consumer.py:203` and `core/lob_recorder.py:140` — outer `except Exception` in the reconnection loop is the fallback after WS-specific exceptions are caught first; intentionally broad to guarantee reconnect. KEEP AS-IS.

## Deferred Items

- DEFERRED: `engine/microstructure_engine.py` — 30 tests in `tests/test_microstructure_engine.py` import and exercise this file; deleting it would drop passing tests below the −3 threshold (443 → ~413); needs a coordinated rewrite of 30 tests to point to `strategy/microstructure.MicrostructureDetector` before removal is safe.

## Rebase Note

*(omitted — REBASE_FALLBACK=false)*

---

## Review Pass 1 — 0 removed, 1 corrected, 0 added

C8 scorer exception tuple updated to include `KeyError` (from `data["model"]` in XGBoostScorer.load). All F, B, C items re-grepped and verified. No __all__ entries, no @validator decorators, no getattr string dispatch in affected symbols. Risk entries for realtime_hub.py:46 and startup_reconciler.py are mitigated (intentional design).

## STATUS: APPROVED

