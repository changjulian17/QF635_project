# Plan Checker Skill

Review a Claude implementation plan that has been pasted or is visible in the current conversation. Evaluate whether the bugs identified are real, whether the proposed fixes are robust and well-integrated, and whether the implementation meets professional standards.

## STRICT CONSTRAINTS

- **DO NOT** implement, edit, or commit anything
- **DO NOT** modify any file in the repository
- Read source files only to verify claims made in the plan — use Read and Bash (`grep`, `git log`, `git diff`) as needed
- Produce a written assessment only

## Workflow

1. **Locate the plan.** The plan is in the current conversation (the user will have pasted it or it will be visible as a prior assistant message). Do not ask the user to paste it again — read it from context.
2. **Ground-truth check.** For each bug or issue the plan claims exists, read the relevant source file(s) to verify the claim is accurate. Note false positives (claimed bugs that do not exist) and false negatives (real bugs the plan missed that are visible in the same files).
3. **Implementation review.** For each proposed fix, evaluate whether it is complete, handles edge cases, avoids regressions, and integrates cleanly with the surrounding code.
4. **Professionalism review.** Assess overall code quality of the proposed changes: naming, error handling, test coverage, comments, and consistency with project conventions.
5. **Produce the report** in the format below.

---

## Report Format

### Plan Summary
One paragraph: what the plan proposes to do and which files/modules it touches.

---

### Section 1 — Bug Validity

For each bug or issue listed in the plan:

| # | Plan's claim | Verdict | Evidence (file:line) |
|---|-------------|---------|----------------------|
| 1 | Short description of the claimed bug | ✅ Confirmed / ❌ False positive / ⚠️ Partially correct | file.py:42 — quote the relevant line |

**Missed bugs (false negatives):** List any real issues visible in the same files that the plan did not catch.

---

### Section 2 — Implementation Robustness

For each proposed fix, answer:

- **Completeness** — Does it fully resolve the root cause, or only the symptom?
- **Edge cases** — Does it handle empty inputs, concurrency, error paths, boundary values?
- **Regression risk** — Could the change break existing behaviour elsewhere? Cite specific callers or tests.
- **Integration** — Does it fit cleanly into the existing architecture (correct layer, correct abstraction, no duplication)?

Format as bullet points under each fix, referencing file:line where relevant.

---

### Section 3 — Fix Quality

Assess the professional quality of the proposed implementation:

- **Naming & style** — Are identifiers, function names, and variable names clear and consistent with the codebase?
- **Error handling** — Are failure paths explicit, logged, and safe (no silent swallows, no bare `except`)?
- **Test coverage** — Does the plan include or reference tests for the fix? Are the tests meaningful (not just happy-path)?
- **Comments & docs** — Are non-obvious decisions explained? Is there unnecessary or redundant commentary?
- **Security** — Does the fix introduce any new attack surfaces (injection, unvalidated input, leaked state)?
- **Performance** — Does the fix introduce any new hot-path costs or memory leaks?

---

### Section 4 — Overall Assessment

| Dimension | Score (1–10) | Notes |
|-----------|-------------|-------|
| Bug validity (are the right problems identified?) | | |
| Implementation robustness (will the fixes hold?) | | |
| Integration quality (fits the architecture?) | | |
| Fix professionalism (production-ready code?) | | |
| Test coverage (fixes are verified?) | | |
| **Overall plan quality** | | |

**Recommendation:** APPROVE PLAN / REVISE BEFORE IMPLEMENTING / REJECT — with a one-sentence justification.

**Top 3 must-fix issues before implementation begins:**
1. ...
2. ...
3. ...
