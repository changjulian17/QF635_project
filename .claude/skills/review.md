# Code Review Skill

Review the current branch diff or a specific PR and produce a structured report with a rating.

## STRICT CONSTRAINTS — never violate these

- **DO NOT** checkout, fetch, or switch branches
- **DO NOT** run `git checkout`, `git restore`, `git switch`, or any command that modifies the working tree
- **DO NOT** stage, commit, or push any files (`git add`, `git commit`, `git push` are forbidden)
- **DO NOT** make any changes to any file in the repository
- This skill is **read-only**: only use Read, Bash (for `git diff`, `git log`, `git show`, `git status`), and GitHub MCP tools to gather information

## Workflow

1. If no PR number is given, use the GitHub MCP tool `mcp__github__list_pull_requests` to list open PRs and pick the most relevant one (or ask the user).
2. Use `mcp__github__pull_request_read` with `method: "get"` for PR metadata and `method: "get_files"` for the file list.
3. For large PRs where `method: "get_diff"` exceeds limits, read key files directly from the working tree using the Read tool or `git show origin/<branch>:<file>`.
4. Analyze the changes and produce the review below.

## Review Format

### Overview
One paragraph describing what the PR does and its overall approach.

### Code Quality
Bullet points covering style consistency, naming, dead code, and readability.

### Correctness & Bugs
Specific bugs or logic errors, each with **file:line** citation.

### Security & Risk
Any security vulnerabilities (injection, auth, secrets exposure) or operational risks.

### Performance
Algorithmic or resource concerns worth noting.

### Test Coverage
Gaps in test coverage relative to the changes made.

### Summary Table

| Severity | Count | Top finding |
|----------|-------|-------------|
| Critical | N | ... |
| Major    | N | ... |
| Minor    | N | ... |

### Rating

Score the PR on a scale of **1–10** across four axes and give an overall score:

| Axis | Score (1–10) | Notes |
|------|-------------|-------|
| Correctness | | |
| Safety / Risk Management | | |
| Test Coverage | | |
| Code Quality | | |
| **Overall** | | |

> **Merge recommendation:** APPROVE / REQUEST CHANGES / NEEDS DISCUSSION
