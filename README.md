# stepflow

Flag SQL Server Agent job steps that never run, watchdogs that find their job by name, and runs
far past their normal duration, from exported `msdb` rows.

A nightly job refreshed a reporting table in one step. That step was set to quit the job
reporting success. Later, someone used a script to append a second step that rebuilt a helper
table. The script did not change the first step, so the second step never ran. Every night the
job history said Succeeded, and the helper table went stale.

A second job was a watchdog. It looked up the nightly job by name and stopped it when a run
hung. Then the nightly job was renamed. The watchdog's lookup found nothing to stop, so the
watchdog did nothing, and it also reported Succeeded every time.

Then a run hung for more than a hundred times its normal duration. When it ended, the history
recorded it as Succeeded, like every other night.

Nothing failed, so nothing alerted. Each of these faults is visible in `msdb`, if something
reads the rows. stepflow reads them offline, from exports, and names each fault.

```
$ python stepflow.py --self-test
stepflow self-test: no server, no network, no credentials
--------------------------------------------------------------------
PASS  a run_duration of 0 is 0 seconds
PASS  run_duration 13542 is 1h 35m 42s, 5742 seconds
PASS  run_duration 253000 is 25h 30m, past the 24 hours the column description stops at  <-- pinned defect
...
PASS  a step appended after a step that quits reporting success is UNREACHABLE  <-- pinned defect
PASS  and the finding names the step before it and its success action
...
PASS  go to the next step on the last step is DANGLING: Agent fails the job there  <-- pinned defect
...
PASS  a start step that was deleted is START_MISSING  <-- pinned defect
...
PASS  a step whose failure quits reporting success is FAIL_REPORTS_SUCCESS  <-- pinned defect
...
PASS  a watchdog whose target was renamed is DISARMED, once  <-- pinned defect
...
PASS  a name in a -- comment is not a binding  <-- pinned defect
PASS  a call inside dynamic SQL is not seen, and no empty name is read  <-- pinned defect
PASS  a name compared on another table is not a job name  <-- pinned defect
...
PASS  a 5 hour run against a 102.5 second median is FAR_PAST_NORMAL  <-- pinned defect
PASS  and it says the run reads as Succeeded, with its ratio
...
PASS  one hung run in the baseline does not hide the next one: a median, not a mean  <-- pinned defect
PASS  when most runs are long, the long run is the normal one
PASS  --baseline-before takes the median from before the spiral, and all six long runs show  <-- pinned defect
...
PASS  a run that reads as Succeeded with a failed step in it is GREEN_OVER_FAILED_STEP  <-- pinned defect
...
PASS  a UTF-16 export, which sqlcmd -u and a PowerShell 5.1 redirect write, reads the same as UTF-8  <-- pinned defect
...
PASS  a command cut at sqlcmd's 256 character default exits 2 and names -y 0  <-- pinned defect
PASS  and it names the line the cut row starts on, not where the reader stopped  <-- pinned defect
...
PASS  a prefix --ap of --apply is refused, and nothing is written  <-- pinned defect
...
PASS  a jobs export with a header and no rows exits 2, not a clean pass  <-- pinned defect
PASS  an empty --steps, an unset shell variable, exits 2 and is not skipped  <-- pinned defect
...
PASS  check() and raises() really do record a failure  <-- pinned defect
PASS  the footer counts a failure and exits 1 on it
PASS  the file imports as a module without running main
--------------------------------------------------------------------
233 assertions, 0 failed
```

The full run prints all 233 assertions. Each `...` line marks a cut in this block.

## What already exists

- **SQL Server Management Studio.** The author of the script below reports that the job dialog
  warns before it saves a step that the success and failure actions skip. A T-SQL change gets
  no warning. That gap is the appended-step story above.
- **"Script to find orphaned/unreachable job steps"** (SQLServerCentral, 2012). It lists a step
  when no step goes to it, the step before it does not go to the next step, and the job does not
  start there. It is short and correct for that one-hop case. Its author notes that it does not
  follow a chain of unreachable steps.
- **dbachecks** (`LongRunningJob` and `LastJobRunTime`). It compares a running job, and a job's
  last run, with that job's average duration over a live connection. The default threshold is
  50 percent over the average. It is a good fit for a team that already runs Pester checks.

stepflow does not need a connection. It walks the whole step graph from the start step, so a
step behind another unreachable step is reported too. It resolves every job that a string literal in a
step command names. It times each run against a median, not an average, because one hung run inflates an
average and hides the next one.

The name `stepflow` is also used by unrelated GitHub projects: a macro-step library, a workflow
engine and an AWS Step Functions dashboard. None of them reads SQL Server Agent.

## Requirements

Python 3.9 or newer and nothing else. No database driver, no third-party package, no network.
You run the SELECTs yourself with `sqlcmd` and give stepflow the CSV files.

The same 233 assertions pass on Windows (Python 3.13.2), Ubuntu (Python 3.12.3) and Windows
(Python 3.9.25). The three runs print the same text, except that Windows ends each line with
CR LF. Branch coverage is 100 percent (`coverage run --branch stepflow.py --self-test`).

```
git clone https://github.com/uhsear/stepflow.git
```

## Usage

1. Print each SELECT into a file.
2. Run each file with `sqlcmd` against `msdb`, with `-y 0`. Add your own connection options.
3. Give stepflow the CSV files.

**The steps export is as sensitive as `msdb`.** It holds the full text of every job step
command, and CmdExec, SSIS and PowerShell steps often carry a password or a connection
string (for example `sqlcmd -P`). stepflow never prints a command, but the CSV file holds
it. Keep `steps.csv` out of any repository and any ticket. `.gitignore` lists the file names
used here, but a different name is not covered. Delete the exports when the run is done.

```
python stepflow.py --sql jobs > jobs.sql
python stepflow.py --sql steps > steps.sql
python stepflow.py --sql history > history.sql
python stepflow.py --sql activity > activity.sql

sqlcmd -S <server> -d msdb -y 0 -f o:65001 -i steps.sql -o steps.csv

python stepflow.py --jobs jobs.csv --steps steps.csv
python stepflow.py --jobs jobs.csv --history history.csv --activity activity.csv
python stepflow.py --jobs jobs.csv --history history.csv --baseline-before 2026-09-01
python stepflow.py --jobs jobs.csv --steps steps.csv --report stepflow.txt --apply
```

| Flag | Default | What it does |
|---|---|---|
| `--jobs` | none | The `sysjobs` export. Required. Every other export is checked against it. |
| `--steps` | off | The `sysjobsteps` export. Turns on the step flow and job reference checks. |
| `--history` | off | The `sysjobhistory` export. Turns on the duration and green-over-failure checks. |
| `--activity` | off | The `sysjobactivity` export. Times the runs that are still going. |
| `--factor` | `10` | A run longer than this many times the median is flagged. Must be more than 1 and finite. |
| `--min-runs` | `5` | The fewest other successful runs a median is taken from. |
| `--min-seconds` | `300` | A run shorter than this is never flagged, whatever its ratio. Must be 0 or more and finite. |
| `--baseline-before` | off | Take the median only from runs that started before this date, `YYYY-MM-DD`. |
| `--report` | off | Write the report to this file. Needs `--apply`. It cannot name an input export. |
| `--apply` | off | Write `--report`. Without it nothing is written. |
| `--sql` | off | Print the SELECT for `jobs`, `steps`, `history` or `activity`, and exit. |
| `--self-test` | off | Run the assertions and exit. It writes fixtures to a temporary directory and removes it. |

Give `--jobs` and at least one other export. Take all the exports at the same time. An export
that names a job id that is not in the jobs export exits 2.

### The SQL, and why `-y 0`

Each SELECT builds its CSV line in T-SQL: it quotes every name and command and doubles every
quote inside them. A job step command holds commas, quotes and line breaks, so a client that
splits on a separator cannot produce a correct CSV. Each line ends in an `eol` column that is
always `$`.

`sqlcmd` cuts every `nvarchar(max)` value at 256 characters unless you give `-y 0`. The cut
loses the closing quote and the `eol` column, and stepflow exits 2 with the line where the cut
row starts. Without `-y 0`, `sqlcmd` also prints a header named `line` and a row of dashes.
stepflow names `-y 0` in both cases.

| Client behaviour | What stepflow does |
|---|---|
| `-y 0` missing | Exits 2, and the message names `-y 0`. |
| `SET NOCOUNT ON` removed from the query | Exits 2 on the `(N rows affected)` line, and names `SET NOCOUNT ON`. |
| `-u` (UTF-16 output) | Reads the file the same as UTF-8. |
| A Windows PowerShell 5.1 `>` redirect (UTF-16) | Reads the file the same as UTF-8. |
| A file in a single-byte code page | Exits 2 if the bytes are not UTF-8, and names `-f o:65001`. |

Microsoft documents that `-W` "can't be used with the `-y` or `-Y` options". Do not add `-W`.

The same page gives `-w` a default of 80 and says that "when an output line exceeds the
specified column width, it wraps on to the next line". The run below used no `-w`, and its
3,972 character row came back whole. So the wrap did not apply to this one-column output on
that build. Other `sqlcmd` builds were not tested. If a command comes back with a line break
that you did not write, add `-w 65535`, the largest width the page allows.

The activity SELECT reads only the newest SQL Server Agent session. It takes that session as
`MAX(session_id)` from `msdb.dbo.syssessions`. Microsoft's example for `sysjobactivity` also
keeps only the newest session, but it takes `MAX(session_id)` from `sysjobactivity` itself. The
two agree while Agent has run a job in the newest session, and differ when it has not. The
SELECT records the server clock as `exported_at`, so a run's age does not depend on the clock
of the machine that runs stepflow.

## What it checks

| Kind | Gate | What it means |
|---|---|---|
| `START_MISSING` | fail | The job's start step does not exist, or the job has no steps. |
| `UNREACHABLE` | fail | No path from the start step reaches this step, on success or on failure. |
| `DANGLING` | fail | A step that runs goes to a step that does not exist. This includes "go to the next step" on the last step. |
| `FAIL_REPORTS_SUCCESS` | fail | When this step fails, the job quits reporting success. |
| `DISARMED` | fail | A step command names a job, by name, pattern or id, and no job matches. |
| `GREEN_OVER_FAILED_STEP` | fail | A run reads as Succeeded, and a step failed in it. A step that is meant to fail and go on to a clean-up step fails the gate on every run. |
| `FAR_PAST_NORMAL` | fail | A finished run took more than `--factor` times the median of the job's other successful runs. |
| `RUNNING_PAST_NORMAL` | fail | A run that is still going has run longer than that. |
| `FAIL_CAN_END_GREEN` | warn | When this step fails, the job goes on to a step, and a path from there quits reporting success. |
| `NAME_BOUND` | warn | A step command finds a job by its name. It works today, and a rename disarms it. |
| `NO_BASELINE` | warn | Some runs were not timed, because the job has too few successful runs to take a median from, or the job has no finished run in the history export. |

A step that never runs is reported once, as `UNREACHABLE`. Its own routes cannot fire, so they
are not reported as well.

`FAIL_CAN_END_GREEN` is a warning because a clean-up step after a failure is a design choice.
When it has really happened, the history shows it as `GREEN_OVER_FAILED_STEP`, which fails.

Each run is timed against the job's other successful runs. A run is never part of its own
median. A long run that a watchdog cancelled is timed too, and the finding says it reads as
Canceled.

### What SQL Server Agent does

The kinds rest on what SQL Server Agent does with each case. Each case below was run once on
SQL Server 2022 CU27 (16.0.4295.3), Developer edition on Linux, in a container, with SQL Server
Agent running. The messages are quoted from `sysjobhistory`. This repository holds no SQL Server
and no script for these runs, so you cannot repeat the table from here. The assertions in the
self-test pin what stepflow does with each case, not what Agent does.

| Case | Observed |
|---|---|
| A step appended after a step that quits reporting success | The job succeeded. "The last step to run was step 1." Step 2 has no history row. |
| "Go to the next step" on the last step, on success | The job failed: "JobManager tried to run a non-existent step (2)". The step itself succeeded. |
| The same on failure | The job failed with the same message. |
| A go-to step 9 in a one-step job | `sp_add_jobstep` printed "Non-existent step referenced by @on_success_step_id." and saved the step. The job failed: "tried to run a non-existent step (9)". |
| The start step deleted with `sp_delete_jobstep` | `start_step_id` stayed 2 with only step 1 left. The job failed before any step ran. |
| A go-to target deleted with `sp_delete_jobstep` | The step that went to it was reset to "quit reporting success". The next run ended after step 1, and reported success. |
| A job with no steps | "Request to run job ... refused because the job has no job steps." |
| A step fails, and its failure action is "quit reporting success" | The step row is Failed. The job outcome is Succeeded. |
| A step fails, goes on to step 2, and step 2 succeeds | The job outcome is Succeeded: "The last step to run was step 2." |
| The watchdog's target renamed with `sp_update_job @new_name` | The watchdog's `IF EXISTS ... WHERE j.name = N'...'` found nothing. The watchdog succeeded, and the target kept running. |
| `sp_stop_job` with the target's name in upper case | Accepted on the default `SQL_Latin1_General_CP1_CI_AS` collation. |
| A step that finds its own job with `$(ESCAPE_NONE(JOBID))`, run after a rename | The step found its job, and succeeded. |

Microsoft documents that `sp_delete_jobstep` "automatically updates the other job steps that
reference the deleted step". It does not document what the update is. The table above shows it.

### Job references

stepflow reads these forms in every step command, of every subsystem:

- `@job_name = N'...'`, as `sp_start_job`, `sp_stop_job` and the other job procedures take it.
- `sp_start_job N'...'` and `sp_stop_job N'...'`, with the name as the first positional value.
- `name = N'...'` and `name LIKE N'...'`, only when the column is the one of `sysjobs` or
  `sysjobs_view`: `j.name` where `j` is an alias of that table, or a bare `name` when it is the
  only table the command reads. `c.name` on `syscategories` is not read.
- `@job_id = '...'`, a literal job id.

A name resolves the way T-SQL `=` compares on a case-insensitive collation: trailing spaces and
case are ignored. A name that matches only when case is ignored is reported with a note that a
case-sensitive `msdb` collation disarms it now. A `LIKE` pattern resolves with `%`, `_` and
`[...]`. A literal job id that exists is not reported, because a rename does not break it.

stepflow cuts out every comment (`--` and nested `/* */`) and every string literal first, and
matches the forms above in the code that is left. A call inside a comment, inside a `PRINT`
text or inside dynamic SQL is not a binding. A name that ends in `+`, such as
`N'Backup - ' + @@SERVERNAME`, is built at run time and is not read. The name that
`sp_add_job` gives a new job is not read, and neither is a lookup inside a `NOT EXISTS (...)`,
because both expect the job to be absent.

A value that holds an Agent token, such as `$(ESCAPE_SQUOTE(JOBNAME))`, is skipped. Agent
replaces the token at run time. For a step that acts on its own job, the token is the fix:
`DECLARE @id uniqueidentifier = $(ESCAPE_NONE(JOBID));` names the job by its id, and a rename
does not change it. A script that creates such a step through `sqlcmd` needs `-x`. Without it,
`sqlcmd` tries to expand the token itself and stops: "'JOBID' scripting variable not defined".

### A run against real exports

These exports came from the SQL Server 2022 container above, through the `--sql` SELECTs and
`sqlcmd -y 0 -f o:65001`. Every job name is synthetic. The times are the server clock.

```
$ python stepflow.py --jobs jobs.csv --steps steps.csv --history history.csv --activity activity.csv
stepflow: 14 job(s), 16 step(s), 46 history row(s), 14 activity row(s)
checked: step flow, job references, run durations, running jobs

FAIL  START_MISSING          sf no steps: the job has no steps. SQL Server Agent refuses to start it.
FAIL  START_MISSING          sf start two: the start step is 2 and the job has steps 1. The job fails before any step runs.
FAIL  UNREACHABLE            sf appended: step 2 (two appended) never runs: step 1 (one) quits reporting success on success and quits reporting failure on failure.
FAIL  UNREACHABLE            sf delete target: step 2 (two) never runs: step 1 (one) quits reporting success on success and quits reporting failure on failure.
FAIL  DANGLING               sf dangling: on success, step 1 (one) goes to step 9, and step 9 does not exist. The job fails there.
FAIL  DANGLING               sf next past end fail: on failure, step 1 (one) goes to the next step, and step 2 does not exist. The job fails there.
FAIL  DANGLING               sf next past end ok: on success, step 1 (one) goes to the next step, and step 2 does not exist. The job fails there.
FAIL  FAIL_REPORTS_SUCCESS   sf fail reports success: when step 1 (one) fails, the job quits reporting success.
FAIL  DISARMED               sf watchdog: step 1 (stop a stuck load) looks for the job named 'sf target load', and no job matches. A rename or a delete disarmed it, and the step can still succeed.
FAIL  GREEN_OVER_FAILED_STEP sf fail reports success: the run started 2026-10-09 11:20:35 reads as Succeeded, and step 1 failed in it.
FAIL  GREEN_OVER_FAILED_STEP sf green over failed: the run started 2026-10-09 11:20:32 reads as Succeeded, and step 1 failed in it.
FAIL  FAR_PAST_NORMAL        sf nightly refresh: the run started 2026-10-09 11:21:40 took 6m 00s, 180x the median 2s of 6 other successful run(s), and reads as Succeeded.
FAIL  RUNNING_PAST_NORMAL    sf hourly load: it has been running for 10m 05s, and its median is 2s over 6 successful run(s).
WARN  FAIL_CAN_END_GREEN     sf green over failed: when step 1 (work) fails, the job goes on to step 2 (tidy), and a path from there quits reporting success.
WARN  NAME_BOUND             sf "quoted", comma job: step 1 (step, with comma) finds the job named 'sf hourly load' by its name. A rename disarms it without an error.
WARN  NO_BASELINE            sf "quoted", comma job: it has no finished run in the history export, so nothing was timed.
WARN  NO_BASELINE            sf appended: 1 run(s) were not timed: fewer than 5 other successful run(s) to take a median from.
...

VERDICT: 13 finding(s) fail the gate, 14 warning(s).
```

The `...` line cuts ten more `NO_BASELINE` warnings: nine for jobs that ran once, and one for
`sf no steps`, which never ran. The same exports give the same report on Windows and on Ubuntu.

The job `sf "quoted", comma job` has a 3,972 character step command with commas, quotes and a
CRLF in it. The export kept all of it, and the job reference at its end was found.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | No finding fails the gate. Warnings may be present. |
| 1 | At least one finding fails the gate. |
| 2 | An export could not be read, is malformed, or disagrees with the jobs export, or `--report` could not be written. |
| 64 | Usage error, including an abbreviated flag such as `--ap`. |

Exit 2 is not exit 1 on purpose. A scheduled check must not file "this export is broken" as "this
job is broken". argparse exits 2 on a usage error by default, so stepflow moves its usage errors
to 64.

## Why the obvious version is wrong

- **One hop is not enough.** A check that asks "does any step go to this step?" passes a step
  whose only caller never runs. stepflow walks the graph from the start step, so every step
  behind an unreachable one is reported.
- **"Go to the next step" is not safe on the last step.** It reads like "carry on". On the last
  step, Agent fails the job, even when the step succeeded.
- **An average hides a hang.** One earlier hung run raises an average enough to pass the next
  one. The self-test pins this with two hung runs: a median flags both.
- **A median can hide a hang too.** When most of the retained runs are long, the long run is the
  median. `--baseline-before` takes the median from a period you know was healthy.
- **The job outcome row is not the whole run.** A run whose outcome is Succeeded can hold a
  failed step. stepflow reads the step rows of each run, not only the outcome row.
- **`run_duration` is not seconds.** It is `HHMMSS` packed into an integer, so `1000` is 10
  minutes, not 1,000 seconds. Microsoft's column description stops at 24 hours, and its own
  example decodes longer runs with the hours above 24. stepflow decodes it the same way.

## Limits

Job references
- It reads job references only in string literals inside step commands. A name held in a
  variable, a parameter, dynamic SQL, or a script outside `msdb` is not seen. A watchdog that is
  a scheduled script, not an Agent job, is outside its view.
- It matches job references with regular expressions, not a T-SQL parser. The comment and
  string rules above cover the common shapes. A shape they do not cover can still be read wrongly.
- A job reference written with double quotes (`@job_name = "x"`, legal when `QUOTED_IDENTIFIER`
  is `OFF`) is not read, so that step is silently not checked.
- A lookup that is meant to find nothing, such as `IF EXISTS (... name LIKE N'Legacy%') PRINT`
  as an inventory check, is reported as `DISARMED`, because stepflow cannot tell it from a
  watchdog. Only a lookup inside `NOT EXISTS` is skipped.
- The `LIKE` ESCAPE clause is not read. A pattern that uses one may resolve wrongly.
- Name resolution assumes a case-insensitive `msdb` collation, which was the default in the
  tested container. A case-only match is still flagged, with a note.

Durations
- It sees only the history that Agent has kept. Agent can be set to trim `sysjobhistory` by
  row count or by age.
- A job with fewer than `--min-runs` other successful runs is never timed, only warned with
  `NO_BASELINE`. A run that hung for many hours still exits 0 if its job has too few retained
  successful runs. With `--activity` and no `--history`, every running job gets `NO_BASELINE`
  and the run exits 0.
- An export that has a header and no rows is read as no data. A header-only jobs export or
  activity export exits 2. A header-only history export warns `NO_BASELINE` for every job. A
  header-only steps export reports `START_MISSING` for every job.
- It reads `sysjobactivity` for the newest Agent session only. A run left open by an earlier
  session is not seen as running.
- Step rows of a run that Agent never closed (a service restart or a crash) have no outcome
  row. The next outcome row takes them, so an old failed step can show as
  `GREEN_OVER_FAILED_STEP` for a later run.
- A history row with `run_date` 0 stops the run with exit 2 ("is not a date and time"). Some
  public `msdb` scripts filter `run_date > 0`, which suggests such rows can exist. This was not
  checked on a real server. Remove the row from the export.
- `msdb` can keep history rows of a job that was deleted with `@delete_history = 0`. Such a
  row names a job id that is not in the jobs export, so the whole run exits 2.
- Times are the server's own clock throughout. stepflow does no time zone conversion.

Exports and output
- The whole export is read into memory, so a very large steps export uses memory in proportion
  to its size. A step command over 131072 characters (the `csv` module's field limit) cannot
  be read and exits 2 with "field larger than field limit".
- An empty path, such as `--steps ""` from an unset shell variable, exits 2. A path that does not
  exist exits 2.
- `--report` with `--apply` overwrites any existing file it names, except an input export,
  which is refused. It does not ask first.
- A job or step name that the output code page cannot encode prints with a `?`, so the report
  does not crash. Set `PYTHONUTF8=1` to print it whole.
- An error message that quotes a cell cuts it to 32 characters, so a shifted column does not
  copy file content into a log. The cut is a safeguard, not a guarantee. Treat the exports and
  the report as sensitive.
- A reader that quits early (`| head -1`) ends the output without a traceback. The exit code
  is still the gate result, but the shell reports the code of the last command in the pipe.

Scope
- It does not read schedules, alerts, operators, or multiserver (master and target) jobs.
- It does not read job step output files or the Agent error log.
- The SQL and every behaviour in "What SQL Server Agent does" were tested on one build: SQL
  Server 2022 CU27 on Linux, with `sqlcmd` 18.6 (ODBC) from the same container. Windows
  `sqlcmd`, the Go-based `sqlcmd`, SQL Server Management Studio exports and Azure SQL Managed
  Instance were not tested.
- There is no `--fix`. Changing a job's steps is an edit somebody signs for.
- Nothing is written without `--apply`, except that `--self-test` writes fixtures to a
  temporary directory and removes it again. stepflow opens no socket and imports no database
  driver, so it holds no credential. The steps export it reads does hold whatever the step
  commands hold. See the warning under Usage.

## Sources

- [dbo.sysjobsteps](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/dbo-sysjobsteps-transact-sql):
  the four values of `on_success_action` and `on_fail_action`, and their defaults.
- [dbo.sysjobs](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/dbo-sysjobs-transact-sql):
  `job_id`, `name` and `start_step_id`.
- [dbo.sysjobhistory](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/dbo-sysjobhistory-transact-sql):
  the `run_status` values, the `yyyyMMdd` and `HHmmss` formats, and the example that decodes
  `run_duration` past 24 hours.
- [dbo.sysjobactivity](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/dbo-sysjobactivity-transact-sql):
  the running test (a start date and no stop date) and the newest-session filter.
- [sp_add_jobstep](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-add-jobstep-transact-sql):
  the action values, and step ids that "start at 1 and increment without gaps".
- [sp_delete_jobstep](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-delete-jobstep-transact-sql):
  "Removing a job step automatically updates the other job steps that reference the deleted step."
- [sp_update_job](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-update-job-transact-sql):
  `@new_name` and `@start_step_id`.
- [Use Tokens in Job Steps](https://learn.microsoft.com/en-us/ssms/agent/use-tokens-in-job-steps):
  the `JOBID` and `JOBNAME` tokens, and the `ESCAPE_NONE` example for a job id.
- [sqlcmd utility](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility): the `-y`
  default of 256, `-W` that "can't be used with the `-y` or `-Y` options", `-f` and `-u`.
- [Resize the Job History Log](https://learn.microsoft.com/en-us/ssms/agent/resize-the-job-history-log):
  the History page settings that trim `sysjobhistory` by row count or by age.
- [Script to find orphaned/unreachable job steps within your SQL jobs](https://www.sqlservercentral.com/?p=29504)
  (SQLServerCentral, 2012): the one-hop check and the SSMS warning it reports.
- [dbachecks](https://github.com/dataplat/dbachecks): `agent.longrunningjob.percentage` and
  `agent.lastjobruntime.percentage`, both 50, in `source/internal/configurations/configuration.ps1`,
  and the average-based query in `source/internal/functions/Get-AllAgentInfo.ps1`.

## Contributing

Open an issue or pull request on GitHub.

## Author

Built by [Asir Khan](https://www.linkedin.com/in/asir-khan-310317264/).

## License

MIT.

## Related

Other single-file tools in this portfolio that pair with this one:

- [taskpulse](https://github.com/uhsear/taskpulse) - the same questions for Windows scheduled
  tasks: silent failures, the configuration that causes them, and a duration baseline from the
  task history.
- [compressfloor](https://github.com/uhsear/compressfloor) - the same pattern of exported system
  rows read offline, for an enterprise geodatabase's compress floor.
- [logsift](https://github.com/uhsear/logsift) - mine a scheduled job's own log files into a
  metric series.
- [jobharness](https://github.com/uhsear/jobharness) - logging, retry, resume and a run lock for
  the script an Agent step calls.
