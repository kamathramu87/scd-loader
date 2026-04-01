---
name: branch-pr
description: Create a new git branch using feat/chore/fix conventions and open a pull request against main. Use when the user wants to start a feature branch, cut a branch, or create a PR.
disable-model-invocation: true
allowed-tools: Bash(git *), Bash(gh *)
---

Create a branch and pull request using these steps:

## Arguments

$ARGUMENTS is optional.

**If $ARGUMENTS is provided:** it should be a short description of the change and optionally a PR title after a colon (e.g. `add scd1 loader: Add SCD1 upsert strategy`).
- If it contains `: `, split on the first `: ` — left side is the description, right side is the PR title.
- Otherwise, the PR title is derived from the description.

**If $ARGUMENTS is empty:** infer the description automatically from the current git changes:
1. Run `git diff --stat HEAD` and `git status --short` to understand what has changed (including untracked files).
2. Run `git diff HEAD` for a brief look at the actual diff content (first 100 lines is enough).
3. Also check `git stash list` in case changes were already stashed.
4. Summarize the changes into a short description (3–6 words) suitable for a branch name.
5. Use that as the description going forward.

## Branch naming

Infer the branch type from the description using these conventions:
- `feat/<slug>` — new feature or capability
- `fix/<slug>` — bug fix
- `chore/<slug>` — maintenance, refactor, tooling, docs, deps

If no changes are detected (clean working tree and no commits ahead of main), tell the user there is nothing to branch from and stop.

Convert the description to a kebab-case slug (lowercase, hyphens only) for the branch name. Propose the branch name to the user and confirm before creating it.

Examples:
- "add scd1 loader" → `feat/add-scd1-loader`
- "fix duplicate upsert flag" → `fix/duplicate-upsert-flag`
- "update ci workflow" → `chore/update-ci-workflow`

## Steps

1. **Stash uncommitted changes** — run `git status` to check for uncommitted changes. If there are staged or unstaged changes, stash them with `git stash` before switching branches. Remind the user to `git stash pop` after the branch is created if needed.

2. **Sync main with remote** — run the following in order:
   - `git checkout main`
   - `git fetch origin`
   - `git merge --ff-only origin/main` to fast-forward local main to remote. If fast-forward fails (diverged history), stop and tell the user to resolve manually.

3. **Create and switch to branch** — run `git checkout -b <branch-name>` from the updated `main`.

4. **Run quality checks** — run `make check` before pushing. If it fails, stop and show the output to the user. Do not push until checks pass.

5. **Push branch to remote** — run `git push -u origin <branch-name>`.

6. **Create draft PR** against `main` using `gh pr create` with:
   - `--draft` flag (so it's not accidentally merged)
   - A title derived from the branch name or the user-provided title
   - A body with the template below
   - Base branch: `main`

PR body template:
```
## Summary
- <!-- describe what this PR does -->

## Test plan
- [ ] Tests pass (`make test`)
- [ ] Quality checks pass (`make check`)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

7. **Print the PR URL** so the user can open it.

## Notes
- Never force-push or use destructive git commands.
- If `gh` is not authenticated, tell the user to run `gh auth login` first.
- If the branch already exists remotely, inform the user and stop.
