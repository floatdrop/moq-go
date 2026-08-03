---
name: moqt-review
description: Review pending changes in this repo for IETF MOQT draft compliance, reinvented stdlib, and CLAUDE.md adherence before committing. Use before any `git commit` in this repo, or when the user asks to double-check spec compliance of a change.
allowed-tools: Bash(git status:*), Bash(git diff:*), Bash(git branch:*), Agent
---

Run the `moqt-reviewer` subagent on the current slice of work and relay its findings.

1. Determine the diff to review:
   - Run `git status` and `git diff HEAD` for uncommitted changes (staged + unstaged).
   - If the working tree is clean (nothing to diff against `HEAD`), diff the current branch against the repo's main branch instead: find it via `git branch -a` / the default remote branch (per this repo's current gitStatus, that's `draft-18`), and use `git diff <main>...HEAD`.
   - If there is truly nothing to review (no uncommitted changes and no commits ahead of main), say so and stop — do not invoke the agent.

2. Launch the `moqt-reviewer` agent (via the `Agent` tool, `subagent_type: moqt-reviewer`) and give it the diff scope you determined in step 1 (pass the actual `git diff` output or the base/head refs — whichever is more token-efficient for the size of the change).

3. Relay the agent's findings to the user as-is. Do not re-review or second-guess them yourself — that's the agent's job, not this skill's.

4. If findings include anything at BLOCKING-equivalent severity (a `spec-compliance` or `claude-md-adherence` finding describing an actual behavioral break, not a style nit), say so plainly before the user commits — don't bury it.
