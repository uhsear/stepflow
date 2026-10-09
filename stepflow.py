#!/usr/bin/env python
"""Flag SQL Server Agent job steps that never run, watchdogs that find their
job by name, and runs far past their normal duration, from exported msdb rows.

A job had one step that quit reporting success. Someone appended a second
step with a script, and the first step still quit reporting success, so the
second step never ran. The job stayed green. A watchdog job looked its target
up by name and stopped it when it hung. The target was renamed, the watchdog
found nothing to stop, and it stayed green too. A run that took more than a
hundred times its normal duration went into the history as Succeeded.

SQL Server Management Studio warns about an unreachable step when the job is
edited in its dialog, but a T-SQL edit gets no warning. A community script
from 2012 finds steps that nothing routes to, one hop deep. dbachecks compares
a running job with its average duration over a live connection. This tool
reads exported msdb rows offline, walks the whole step graph from the start
step, resolves every job named in a step command, and times each finished or
running run against the median of the job's other successful runs.

    python stepflow.py --self-test
    python stepflow.py --sql steps > steps.sql
    python stepflow.py --jobs jobs.csv --steps steps.csv
    python stepflow.py --jobs jobs.csv --history history.csv --activity activity.csv
    python stepflow.py --jobs jobs.csv --steps steps.csv --report out.txt --apply

Nothing is written without --apply. Nothing connects to a server.

Exit codes: 0 no finding fails the gate, 1 at least one does, 2 an export
could not be read or is malformed, 64 usage error.
"""

from __future__ import print_function

import argparse
import contextlib
import csv
import datetime
import importlib
import io
import os
import re
import shutil
import statistics
import sys
import tempfile

# =============================================================================
# CONFIGURATION. The defaults of the three timing flags.
# =============================================================================

# A run is far past normal when it takes more than this many times the median
# of the job's other successful runs. A median, not a mean: one earlier hung
# run inflates a mean and hides the next one.
DEFAULT_FACTOR = 10.0

# The fewest other successful runs a median is taken from. With fewer, a run
# is not timed, and the job is listed as NO_BASELINE instead.
DEFAULT_MIN_RUNS = 5

# A run shorter than this is never far past normal, whatever its ratio. A job
# that normally takes 1 second and once took 12 is not an incident.
DEFAULT_MIN_SECONDS = 300

# on_success_action and on_fail_action values (dbo.sysjobsteps).
QUIT_SUCCESS, QUIT_FAILURE, NEXT_STEP, GO_TO_STEP = 1, 2, 3, 4

# run_status values (dbo.sysjobhistory).
RUN_STATUS = {0: "Failed", 1: "Succeeded", 2: "Retry", 3: "Canceled",
              4: "In Progress"}

# Finding kinds, in report order. Every kind in GATE fails the run (exit 1).
GATE = ("START_MISSING", "UNREACHABLE", "DANGLING", "FAIL_REPORTS_SUCCESS",
        "DISARMED", "GREEN_OVER_FAILED_STEP", "FAR_PAST_NORMAL",
        "RUNNING_PAST_NORMAL")
WARN = ("FAIL_CAN_END_GREEN", "NAME_BOUND", "NO_BASELINE")
KINDS = GATE + WARN

# The SELECTs that --sql prints. Each one builds its CSV line in T-SQL, so no
# client setting can split a field: a command holds commas, quotes and line
# breaks. The eol column is a constant "$". A row without it was cut short,
# which is what sqlcmd does to an nvarchar(max) value without -y 0.
SQL = {
    "jobs": """SET NOCOUNT ON;
SELECT line FROM (
  SELECT 0 AS k, CAST(N'job_id,name,start_step_id,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(CONVERT(char(36), j.job_id), N',"',
                   REPLACE(j.name, N'"', N'""'), N'",',
                   j.start_step_id, N',$')
  FROM msdb.dbo.sysjobs AS j
) AS t ORDER BY k;
""",
    "steps": """SET NOCOUNT ON;
SELECT line FROM (
  SELECT 0 AS k, CAST(N'job_id,step_id,step_name,on_success_action,on_success_step_id,on_fail_action,on_fail_step_id,command,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(CONVERT(char(36), s.job_id), N',', s.step_id, N',"',
                   REPLACE(s.step_name, N'"', N'""'), N'",',
                   s.on_success_action, N',', s.on_success_step_id, N',',
                   s.on_fail_action, N',', s.on_fail_step_id, N',"',
                   REPLACE(CAST(ISNULL(s.command, N'') AS nvarchar(max)), N'"', N'""'),
                   N'",$')
  FROM msdb.dbo.sysjobsteps AS s
) AS t ORDER BY k;
""",
    "history": """SET NOCOUNT ON;
SELECT line FROM (
  SELECT 0 AS k, CAST(N'instance_id,job_id,step_id,run_status,run_date,run_time,run_duration,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(h.instance_id, N',', CONVERT(char(36), h.job_id), N',',
                   h.step_id, N',', h.run_status, N',', h.run_date, N',',
                   h.run_time, N',', h.run_duration, N',$')
  FROM msdb.dbo.sysjobhistory AS h
) AS t ORDER BY k;
""",
    "activity": """SET NOCOUNT ON;
SELECT line FROM (
  SELECT 0 AS k, CAST(N'job_id,start_execution_date,stop_execution_date,exported_at,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(CONVERT(char(36), a.job_id), N',',
                   CONVERT(char(19), a.start_execution_date, 120), N',',
                   CONVERT(char(19), a.stop_execution_date, 120), N',',
                   CONVERT(char(19), GETDATE(), 120), N',$')
  FROM msdb.dbo.sysjobactivity AS a
  WHERE a.session_id = (SELECT MAX(s.session_id) FROM msdb.dbo.syssessions AS s)
) AS t ORDER BY k;
""",
}

COLUMNS = {
    "jobs": ("job_id", "name", "start_step_id"),
    "steps": ("job_id", "step_id", "step_name", "on_success_action",
              "on_success_step_id", "on_fail_action", "on_fail_step_id",
              "command"),
    "history": ("instance_id", "job_id", "step_id", "run_status", "run_date",
                "run_time", "run_duration"),
    "activity": ("job_id", "start_execution_date", "stop_execution_date",
                 "exported_at"),
}


# ---------------------------------------------------------------- the model

class Job(object):
    def __init__(self, job_id, name, start_step_id):
        self.job_id = job_id.lower()
        self.name = name
        self.start_step_id = start_step_id


class Step(object):
    def __init__(self, job_id, step_id, name, on_success_action=QUIT_SUCCESS,
                 on_success_step_id=0, on_fail_action=QUIT_FAILURE,
                 on_fail_step_id=0, command=""):
        self.job_id = job_id.lower()
        self.step_id = step_id
        self.name = name
        self.on_success_action = on_success_action
        self.on_success_step_id = on_success_step_id
        self.on_fail_action = on_fail_action
        self.on_fail_step_id = on_fail_step_id
        self.command = command


class HistoryRow(object):
    def __init__(self, instance_id, job_id, step_id, run_status, start,
                 seconds):
        self.instance_id = instance_id
        self.job_id = job_id.lower()
        self.step_id = step_id
        self.run_status = run_status
        self.start = start
        self.seconds = seconds


class Activity(object):
    def __init__(self, job_id, start, stop, exported_at):
        self.job_id = job_id.lower()
        self.start = start
        self.stop = stop
        self.exported_at = exported_at


class Finding(object):
    def __init__(self, kind, job, text):
        self.kind = kind
        self.job = job
        self.text = text

    @property
    def gate(self):
        return self.kind in GATE


class Report(object):
    def __init__(self, findings, counts, checked):
        self.findings = findings
        self.counts = counts
        self.checked = checked

    @property
    def failing(self):
        return [f for f in self.findings if f.gate]

    @property
    def warnings(self):
        return [f for f in self.findings if not f.gate]


# ------------------------------------------------------------- the decisions

def decode_duration(value):
    """Seconds from a run_duration: HHMMSS, and the hours can pass 99."""
    if value < 0:
        raise ValueError("run_duration %d is negative" % value)
    hours, rest = divmod(value, 10000)
    minutes, seconds = divmod(rest, 100)
    if minutes > 59 or seconds > 59:
        raise ValueError("run_duration %d is not in HHMMSS form" % value)
    return hours * 3600 + minutes * 60 + seconds


def run_start(run_date, run_time):
    """The start of a run from run_date (yyyyMMdd) and run_time (HHmmss)."""
    year, month_day = divmod(run_date, 10000)
    month, day = divmod(month_day, 100)
    hour, minute_second = divmod(run_time, 10000)
    minute, second = divmod(minute_second, 100)
    try:
        return datetime.datetime(year, month, day, hour, minute, second)
    except ValueError:
        raise ValueError("run_date %d with run_time %d is not a date and time"
                         % (run_date, run_time))


def fmt_seconds(seconds):
    seconds = int(round(seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return "%dh %02dm %02ds" % (hours, minutes, secs)
    if minutes:
        return "%dm %02ds" % (minutes, secs)
    return "%ds" % secs


def describe_action(action, goto):
    if action == QUIT_SUCCESS:
        return "quits reporting success"
    if action == QUIT_FAILURE:
        return "quits reporting failure"
    if action == NEXT_STEP:
        return "goes to the next step"
    return "goes to step %d" % goto


def target_of(step, action, goto):
    """The step id an action leads to, or None when the action ends the job."""
    if action == NEXT_STEP:
        return step.step_id + 1
    if action == GO_TO_STEP:
        return goto
    return None


def _edges(step):
    return ((step.on_success_action, step.on_success_step_id),
            (step.on_fail_action, step.on_fail_step_id))


def reachable(by_id, start):
    """Every step id a run can reach from start, along either outcome."""
    seen = set()
    todo = [start]
    while todo:
        sid = todo.pop()
        if sid in seen or sid not in by_id:
            continue
        seen.add(sid)
        step = by_id[sid]
        for action, goto in _edges(step):
            target = target_of(step, action, goto)
            if target is not None:
                todo.append(target)
    return seen


def can_end_green(by_id, start):
    """True when some path from start reaches a quit reporting success."""
    for sid in reachable(by_id, start):
        step = by_id[sid]
        if QUIT_SUCCESS in (step.on_success_action, step.on_fail_action):
            return True
    return False


def analyse_flow(job, steps):
    """Findings for one job's step graph. steps are that job's steps only."""
    by_id = dict((s.step_id, s) for s in steps)
    if not by_id:
        return [Finding("START_MISSING", job.name,
                        "the job has no steps. SQL Server Agent refuses to "
                        "start it.")]
    if job.start_step_id not in by_id:
        return [Finding("START_MISSING", job.name,
                        "the start step is %d and the job has steps %s. The "
                        "job fails before any step runs."
                        % (job.start_step_id,
                           ", ".join(str(i) for i in sorted(by_id))))]
    seen = reachable(by_id, job.start_step_id)
    findings = []
    for sid in sorted(by_id):
        step = by_id[sid]
        if sid not in seen:
            # A step that never runs is reported once, here. Its own routes
            # cannot fire, so they are not reported as well.
            prev = by_id.get(sid - 1)
            if prev is not None and prev.step_id in seen:
                why = ("step %d (%s) %s on success and %s on failure"
                       % (prev.step_id, prev.name,
                          describe_action(prev.on_success_action,
                                          prev.on_success_step_id),
                          describe_action(prev.on_fail_action,
                                          prev.on_fail_step_id)))
            else:
                why = "no step that runs goes to it"
            findings.append(Finding(
                "UNREACHABLE", job.name,
                "step %d (%s) never runs: %s." % (sid, step.name, why)))
            continue
        for outcome, (action, goto) in zip(("success", "failure"),
                                           _edges(step)):
            target = target_of(step, action, goto)
            if target is not None and target not in by_id:
                findings.append(Finding(
                    "DANGLING", job.name,
                    "on %s, step %d (%s) %s, and step %d does not exist. The "
                    "job fails there." % (outcome, sid, step.name,
                                          describe_action(action, goto),
                                          target)))
        if step.on_fail_action == QUIT_SUCCESS:
            findings.append(Finding(
                "FAIL_REPORTS_SUCCESS", job.name,
                "when step %d (%s) fails, the job quits reporting success."
                % (sid, step.name)))
            continue
        target = target_of(step, step.on_fail_action, step.on_fail_step_id)
        if target in by_id and can_end_green(by_id, target):
            findings.append(Finding(
                "FAIL_CAN_END_GREEN", job.name,
                "when step %d (%s) fails, the job goes on to step %d (%s), "
                "and a path from there quits reporting success."
                % (sid, step.name, target, by_id[target].name)))
    return findings


# Matches a T-SQL string literal, N'...' or '...', with '' as an escaped quote.
_LITERAL = r"N?'((?:[^']|'')*)'"
_NAME_ARG = re.compile(r"@job_name\s*=\s*" + _LITERAL, re.I)
_POSITIONAL = re.compile(r"\bsp_(?:start|stop)_job\s+" + _LITERAL, re.I)
_NAME_FILTER = re.compile(r"\bname\s*(=|like)\s*" + _LITERAL, re.I)
_ID_ARG = re.compile(r"@job_id\s*=\s*N?'?\{?([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}"
                     r"[0-9a-f]{12})\}?'?", re.I)
_SYSJOBS = re.compile(r"\bsysjobs\b|\bsysjobs_view\b", re.I)


def job_references(text):
    """The jobs a command names, as (kind, value) pairs in first-seen order.

    kind is "name", "like" or "id". A name compared in a WHERE clause counts
    only when the command reads msdb's sysjobs, because "name" is the most
    common column name there is. A value holding an Agent token such as
    $(ESCAPE_SQUOTE(JOBNAME)) is resolved at run time, so it is skipped.
    """
    found = []

    def add(kind, value):
        value = value.replace("''", "'")
        if "$(" in value:
            return
        if (kind, value) not in found:
            found.append((kind, value))

    for match in _NAME_ARG.finditer(text):
        add("name", match.group(1))
    for match in _POSITIONAL.finditer(text):
        add("name", match.group(1))
    if _SYSJOBS.search(text):
        for match in _NAME_FILTER.finditer(text):
            add("like" if match.group(1).lower() == "like" else "name",
                match.group(2))
    for match in _ID_ARG.finditer(text):
        add("id", match.group(1).lower())
    return found


def like_regex(pattern):
    """A regex that matches what T-SQL LIKE matches, ignoring case.

    % and _ are wildcards and [...] is a character class. The ESCAPE clause
    is not read.
    """
    out = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        close = pattern.find("]", i + 2) if char == "[" else -1
        if char == "%":
            out.append(".*")
        elif char == "_":
            out.append(".")
        elif close != -1:
            body = pattern[i + 1:close]
            negate = body.startswith("^")
            if negate:
                body = body[1:]
            out.append("[%s%s]" % ("^" if negate else "", "".join(
                "-" if c == "-" else re.escape(c) for c in body)))
            i = close
        else:
            out.append(re.escape(char))
        i += 1
    return re.compile("".join(out) + r"\Z", re.I | re.S)


def resolve(kind, value, jobs):
    """(matching job names, True when a name matches only ignoring case)."""
    if kind == "id":
        return [j.name for j in jobs if j.job_id == value], False
    if kind == "like":
        rx = like_regex(value)
        return [j.name for j in jobs if rx.match(j.name)], False
    # T-SQL = ignores trailing spaces, so they are ignored here too.
    want = value.rstrip()
    exact = [j.name for j in jobs if j.name.rstrip() == want]
    if exact:
        return exact, False
    folded = [j.name for j in jobs if j.name.rstrip().lower() == want.lower()]
    return folded, bool(folded)


def analyse_references(jobs, steps):
    """DISARMED and NAME_BOUND findings for every job a step command names."""
    names = dict((j.job_id, j.name) for j in jobs)
    findings = []
    for step in steps:
        for kind, value in job_references(step.command):
            matched, case_only = resolve(kind, value, jobs)
            where = "step %d (%s)" % (step.step_id, step.name)
            what = {"name": "the job named '%s'",
                    "like": "jobs LIKE '%s'",
                    "id": "the job with id %s"}[kind] % value
            if not matched:
                findings.append(Finding(
                    "DISARMED", names[step.job_id],
                    "%s looks for %s, and no job matches. A rename or a "
                    "delete disarmed it, and the step can still succeed."
                    % (where, what)))
            elif kind != "id":
                note = ""
                if case_only:
                    note = (" It matches only when case is ignored, so a "
                            "case-sensitive msdb collation disarms it now.")
                elif kind == "like":
                    note = " It matches: %s." % ", ".join(
                        "'%s'" % n for n in sorted(matched))
                findings.append(Finding(
                    "NAME_BOUND", names[step.job_id],
                    "%s finds %s by its name. A rename disarms it without an "
                    "error.%s" % (where, what, note)))
    return findings


def runs_of(rows):
    """One job's history as (outcome row, step rows) pairs, oldest first.

    Agent writes the step rows of a run before its outcome row (step_id 0),
    and never runs one job twice at once, so instance_id order groups them.
    Step rows after the last outcome row belong to a run still going.
    """
    runs = []
    pending = []
    for row in sorted(rows, key=lambda r: r.instance_id):
        if row.step_id == 0:
            runs.append((row, pending))
            pending = []
        else:
            pending.append(row)
    return runs


def analyse_history(jobs, history, factor=DEFAULT_FACTOR,
                    min_runs=DEFAULT_MIN_RUNS, min_seconds=DEFAULT_MIN_SECONDS,
                    baseline_before=None):
    """(findings, baselines). baselines maps job_id to (median, run count)."""
    by_job = {}
    for row in history:
        by_job.setdefault(row.job_id, []).append(row)
    findings = []
    baselines = {}
    for job in jobs:
        runs = [r for r in runs_of(by_job.get(job.job_id, []))
                if r[0].run_status != 4]
        pool = [o for o, _ in runs if o.run_status == 1 and (
            baseline_before is None or o.start < baseline_before)]
        if len(pool) >= min_runs:
            baselines[job.job_id] = (statistics.median(
                [o.seconds for o in pool]), len(pool))
        untimed = 0
        for outcome, step_rows in runs:
            started = outcome.start.strftime("%Y-%m-%d %H:%M:%S")
            failed = [s for s in step_rows if s.run_status == 0]
            if outcome.run_status == 1 and failed:
                findings.append(Finding(
                    "GREEN_OVER_FAILED_STEP", job.name,
                    "the run started %s reads as Succeeded, and step %s "
                    "failed in it." % (started, ", ".join(
                        str(s.step_id) for s in failed))))
            # The run itself never counts toward its own normal.
            others = [o.seconds for o in pool
                      if o.instance_id != outcome.instance_id]
            if len(others) < min_runs:
                untimed += 1
                continue
            normal = statistics.median(others)
            if outcome.seconds >= min_seconds and \
                    outcome.seconds > factor * normal:
                ratio = ("%.0fx" % (outcome.seconds / normal) if normal
                         else "past")
                findings.append(Finding(
                    "FAR_PAST_NORMAL", job.name,
                    "the run started %s took %s, %s the median %s of %d "
                    "other successful run(s), and reads as %s."
                    % (started, fmt_seconds(outcome.seconds), ratio,
                       fmt_seconds(normal), len(others),
                       RUN_STATUS.get(outcome.run_status, "status %d"
                                      % outcome.run_status))))
        if untimed:
            findings.append(Finding(
                "NO_BASELINE", job.name,
                "%d run(s) were not timed: fewer than %d other successful "
                "run(s) to take a median from." % (untimed, min_runs)))
    return findings, baselines


def analyse_activity(jobs, activity, baselines, factor=DEFAULT_FACTOR,
                     min_seconds=DEFAULT_MIN_SECONDS):
    """RUNNING_PAST_NORMAL and NO_BASELINE findings for runs not finished."""
    names = dict((j.job_id, j.name) for j in jobs)
    findings = []
    for row in activity:
        if row.start is None or row.stop is not None:
            continue
        elapsed = (row.exported_at - row.start).total_seconds()
        name = names[row.job_id]
        if elapsed < 0:
            raise ValueError("job '%s' starts after the export time" % name)
        base = baselines.get(row.job_id)
        if base is None:
            findings.append(Finding(
                "NO_BASELINE", name,
                "it has been running for %s, with no median to compare."
                % fmt_seconds(elapsed)))
        elif elapsed >= min_seconds and elapsed > factor * base[0]:
            findings.append(Finding(
                "RUNNING_PAST_NORMAL", name,
                "it has been running for %s, and its median is %s over %d "
                "successful run(s)." % (fmt_seconds(elapsed),
                                        fmt_seconds(base[0]), base[1])))
    return findings


def check_tables(jobs, steps, history, activity):
    """Refuse exports that disagree with each other. Raises ValueError."""
    ids = set()
    for job in jobs:
        if job.job_id in ids:
            raise ValueError("job id %s is in the jobs export twice"
                             % job.job_id)
        ids.add(job.job_id)
    keys = set()
    for name, rows in (("steps", steps), ("history", history),
                       ("activity", activity)):
        for row in rows:
            if row.job_id not in ids:
                raise ValueError(
                    "the %s export names job id %s, which is not in the jobs "
                    "export. Take every export at the same time."
                    % (name, row.job_id))
    for step in steps:
        key = (step.job_id, step.step_id)
        if key in keys:
            raise ValueError("job id %s has step %d twice in the steps "
                             "export" % key)
        keys.add(key)


def build_report(jobs, steps=None, history=None, activity=None,
                 factor=DEFAULT_FACTOR, min_runs=DEFAULT_MIN_RUNS,
                 min_seconds=DEFAULT_MIN_SECONDS, baseline_before=None):
    """Run every check the supplied exports allow. None means not supplied."""
    check_tables(jobs, steps or [], history or [], activity or [])
    findings = []
    checked = []
    if steps is not None:
        checked.extend(["step flow", "job references"])
        for job in jobs:
            findings.extend(analyse_flow(
                job, [s for s in steps if s.job_id == job.job_id]))
        findings.extend(analyse_references(jobs, steps))
    baselines = {}
    if history is not None:
        checked.append("run durations")
        found, baselines = analyse_history(jobs, history, factor, min_runs,
                                           min_seconds, baseline_before)
        findings.extend(found)
    if activity is not None:
        checked.append("running jobs")
        findings.extend(analyse_activity(jobs, activity, baselines, factor,
                                         min_seconds))
    findings.sort(key=lambda f: (KINDS.index(f.kind), f.job.lower(), f.text))
    counts = (len(jobs), len(steps or []), len(history or []),
              len(activity or []))
    return Report(findings, counts, checked)


def describe(report):
    lines = ["stepflow: %d job(s), %d step(s), %d history row(s), %d activity "
             "row(s)" % report.counts,
             "checked: %s" % ", ".join(report.checked), ""]
    for f in report.findings:
        lines.append("%s  %-22s %s: %s" % ("FAIL" if f.gate else "WARN",
                                           f.kind, f.job, f.text))
    if report.findings:
        lines.append("")
    if report.failing:
        lines.append("VERDICT: %d finding(s) fail the gate, %d warning(s)."
                     % (len(report.failing), len(report.warnings)))
    else:
        lines.append("VERDICT: no finding fails the gate, %d warning(s)."
                     % len(report.warnings))
    return lines


def gate(report):
    return 1 if report.failing else 0


# --------------------------------------------------------------- self-test

def self_test():
    """Assertions over the decision core and the CLI. No server, no network."""
    passed = [0]
    failed = []

    def check(cond, label):
        if cond:
            passed[0] += 1
            print("PASS  %s" % label)
        else:
            failed.append(label)
            print("FAIL  %s" % label)

    def raises(fn, label):
        try:
            fn()
        except ValueError:
            check(True, label)
        except Exception as exc:
            check(False, "%s (wrong exception %r)" % (label, exc))
        else:
            check(False, "%s (no error raised)" % label)

    def kinds(findings):
        return [f.kind for f in findings]

    print("stepflow self-test: no server, no network, no credentials")
    print("-" * 68)

    # ---- run_duration and run_date/run_time
    check(decode_duration(0) == 0, "a run_duration of 0 is 0 seconds")
    check(decode_duration(13542) == 5742,
          "run_duration 13542 is 1h 35m 42s, 5742 seconds")
    check(decode_duration(253000) == 91800,
          "run_duration 253000 is 25h 30m, past the 24 hours the column "
          "description stops at  <-- pinned defect")
    check(decode_duration(1000000) == 360000,
          "run_duration 1000000 is 100 hours, not 10")
    raises(lambda: decode_duration(-1), "a negative run_duration is refused")
    raises(lambda: decode_duration(160), "run_duration 160 has 60 seconds and "
           "is refused")
    raises(lambda: decode_duration(6000), "run_duration 6000 has 60 minutes "
           "and is refused")
    check(run_start(20261009, 72105) ==
          datetime.datetime(2026, 10, 9, 7, 21, 5),
          "run_time 72105 is 07:21:05, the leading zero dropped by an int")
    raises(lambda: run_start(20261332, 0), "a month of 13 is refused")
    raises(lambda: run_start(20261009, 250000), "an hour of 25 is refused")
    check(fmt_seconds(45) == "45s" and fmt_seconds(360) == "6m 00s" and
          fmt_seconds(11700) == "3h 15m 00s" and fmt_seconds(2.5) == "2s",
          "durations print as h, m and s, rounded to the second")

    # ---- the step graph
    jid = "0a0a0a0a-0000-4000-8000-000000000001"

    def job(start=1, name="nightly"):
        return Job(jid, name, start)

    def step(sid, ok=QUIT_SUCCESS, ok_id=0, fail=QUIT_FAILURE, fail_id=0,
             name=None, command=""):
        return Step(jid, sid, name or "s%d" % sid, ok, ok_id, fail, fail_id,
                    command)

    found = analyse_flow(job(), [step(1, NEXT_STEP), step(2, NEXT_STEP),
                                 step(3)])
    check(found == [], "three steps that each go to the next one are clean")

    found = analyse_flow(job(), [step(1, name="refresh"),
                                 step(2, name="rebuild helper")])
    check(kinds(found) == ["UNREACHABLE"],
          "a step appended after a step that quits reporting success is "
          "UNREACHABLE  <-- pinned defect")
    check("step 2 (rebuild helper) never runs" in found[0].text and
          "step 1 (refresh) quits reporting success on success" in
          found[0].text,
          "and the finding names the step before it and its success action")
    check(found[0].gate, "and UNREACHABLE fails the gate")

    found = analyse_flow(job(), [step(1, GO_TO_STEP, 3), step(2), step(3)])
    check(kinds(found) == ["UNREACHABLE"] and
          "step 1 (s1) quits reporting success on success" not in
          found[0].text and "goes to step 3 on success" in found[0].text,
          "a go-to that jumps over a step leaves it UNREACHABLE")

    found = analyse_flow(job(), [step(1), step(2, NEXT_STEP), step(3)])
    check(kinds(found) == ["UNREACHABLE", "UNREACHABLE"],
          "a chain after a quitting step is UNREACHABLE at every link")
    check("no step that runs goes to it" in found[1].text,
          "and the second link says nothing that runs goes to it")

    found = analyse_flow(job(), [step(1, NEXT_STEP)])
    check(kinds(found) == ["DANGLING"],
          "go to the next step on the last step is DANGLING: Agent fails the "
          "job there  <-- pinned defect")
    check("step 2 does not exist" in found[0].text and
          "on success" in found[0].text,
          "and the finding names the step that does not exist")

    found = analyse_flow(job(), [step(1, fail=NEXT_STEP)])
    check(kinds(found) == ["DANGLING"] and "on failure" in found[0].text,
          "go to the next step on failure of the last step is DANGLING too")

    found = analyse_flow(job(), [step(1, GO_TO_STEP, 9)])
    check(kinds(found) == ["DANGLING"] and "goes to step 9" in found[0].text,
          "a go-to a step that does not exist is DANGLING  <-- pinned defect")

    found = analyse_flow(job(start=2), [step(1)])
    check(kinds(found) == ["START_MISSING"],
          "a start step that was deleted is START_MISSING  <-- pinned defect")
    check("start step is 2" in found[0].text and "steps 1" in found[0].text,
          "and it names the start step and the steps there are")
    check(len(found) == 1,
          "and the steps it strands are not also listed as UNREACHABLE")

    found = analyse_flow(job(), [])
    check(kinds(found) == ["START_MISSING"] and "no steps" in found[0].text,
          "a job with no steps is START_MISSING: Agent refuses to start it")

    found = analyse_flow(job(start=2), [step(1), step(2, NEXT_STEP)])
    check(kinds(found) == ["UNREACHABLE", "DANGLING"],
          "a start step past step 1 strands step 1")

    found = analyse_flow(job(), [step(1, fail=QUIT_SUCCESS)])
    check(kinds(found) == ["FAIL_REPORTS_SUCCESS"],
          "a step whose failure quits reporting success is "
          "FAIL_REPORTS_SUCCESS  <-- pinned defect")
    check(found[0].gate, "and it fails the gate")

    found = analyse_flow(job(), [step(1, NEXT_STEP, fail=NEXT_STEP),
                                 step(2)])
    check(kinds(found) == ["FAIL_CAN_END_GREEN"],
          "a failure that goes on to a step that quits reporting success is "
          "FAIL_CAN_END_GREEN")
    check(not found[0].gate,
          "and it is a warning, because a clean-up step is a design choice")

    found = analyse_flow(job(), [step(1, NEXT_STEP, fail=GO_TO_STEP,
                                      fail_id=2),
                                 step(2, QUIT_FAILURE)])
    check(found == [],
          "a failure routed to a step that quits reporting failure is clean")

    found = analyse_flow(job(), [step(1, NEXT_STEP, fail=GO_TO_STEP,
                                      fail_id=1),
                                 step(2, QUIT_FAILURE)])
    check(found == [], "a step that retries itself on failure does not hang "
          "the walk")

    found = analyse_flow(job(), [step(1, NEXT_STEP), step(2, GO_TO_STEP, 1)])
    check(found == [], "a loop between two steps does not hang the walk")

    found = analyse_flow(job(), [step(1), step(2, GO_TO_STEP, 9,
                                                 fail=QUIT_SUCCESS)])
    check(kinds(found) == ["UNREACHABLE"],
          "an unreachable step's own routes are not reported: they cannot "
          "fire")

    found = analyse_flow(job(), [step(1, NEXT_STEP, fail=GO_TO_STEP,
                                      fail_id=7), step(2)])
    check(kinds(found) == ["DANGLING"] and "on failure" in found[0].text,
          "a failure routed to a missing step is DANGLING and not green")

    check(can_end_green({1: step(1, QUIT_FAILURE, fail=GO_TO_STEP,
                                 fail_id=9)}, 1) is False,
          "a walk that ends at a missing step cannot end green")

    # ---- job references in step commands
    refs = job_references(
        "EXEC msdb.dbo.sp_stop_job @job_name = N'nightly load';")
    check(refs == [("name", "nightly load")],
          "@job_name = N'...' is read as a job name")
    check(job_references("EXEC sp_start_job 'it''s load'") ==
          [("name", "it's load")],
          "a positional sp_start_job name is read, with '' as one quote")
    check(job_references("exec SP_STOP_JOB N'x'; exec sp_stop_job N'x'") ==
          [("name", "x")], "case is ignored and a repeated name is kept once")
    text = ("IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs AS j WHERE "
            "j.name = N'nightly load')")
    check(job_references(text) == [("name", "nightly load")],
          "a name compared against sysjobs is read")
    check(job_references("SELECT * FROM dbo.parks WHERE name = 'x'") == [],
          "the same comparison on a table that is not sysjobs is not read")
    check(job_references("SELECT name FROM msdb.dbo.sysjobs_view WHERE name "
                         "LIKE N'%load%'") == [("like", "%load%")],
          "a LIKE against sysjobs_view is read as a pattern")
    check(job_references("FROM msdb.dbo.sysjobsteps s WHERE s.step_name = "
                         "'x'") == [],
          "step_name is not name, and sysjobsteps is not sysjobs")
    check(job_references(
        "EXEC sp_stop_job @job_name = N'$(ESCAPE_SQUOTE(JOBNAME))'") == [],
          "a JOBNAME token is resolved at run time, so it is not a binding")
    check(job_references("SET @id = $(ESCAPE_NONE(JOBID)); EXEC sp_stop_job "
                         "@job_id = @id") == [],
          "a JOBID token and a variable are not read")
    gid = "5D1B2C3E-0000-4000-8000-00000000ABCD"
    check(job_references("EXEC sp_start_job @job_id = '{%s}'" % gid) ==
          [("id", gid.lower())], "a literal @job_id is read, braces or not")

    # ---- LIKE
    check(like_regex("%load%").match("Nightly LOAD v2") is not None,
          "LIKE '%load%' matches across case")
    check(like_regex("load_").match("load1") is not None and
          like_regex("load_").match("load") is None,
          "_ matches exactly one character")
    check(like_regex("job[0-9]").match("job7") is not None and
          like_regex("job[0-9]").match("jobx") is None,
          "[0-9] is a character range")
    check(like_regex("job[^0-9]").match("jobx") is not None and
          like_regex("job[^0-9]").match("job7") is None,
          "[^0-9] is a negated range")
    check(like_regex("a.b(c)").match("a.b(c)") is not None and
          like_regex("a.b(c)").match("aXb(c)") is None,
          "regex characters in a pattern are literal")
    check(like_regex("x[]").match("x[]") is not None and
          like_regex("[ab").match("[ab") is not None,
          "an empty or unclosed bracket is literal")

    # ---- resolving references
    jobs = [Job("00000000-0000-4000-8000-000000000001", "nightly load v2", 1),
            Job("00000000-0000-4000-8000-000000000002", "watchdog", 1),
            Job("00000000-0000-4000-8000-000000000003", "Hourly Load", 1)]
    check(resolve("name", "nightly load", jobs) == ([], False),
          "a renamed target resolves to nothing")
    check(resolve("name", "nightly load v2  ", jobs) ==
          (["nightly load v2"], False),
          "trailing spaces are ignored, as T-SQL = ignores them")
    check(resolve("name", "hourly load", jobs) == (["Hourly Load"], True),
          "a match only without case is flagged as case-only")
    check(resolve("like", "%load%", jobs) ==
          (["nightly load v2", "Hourly Load"], False),
          "a LIKE pattern resolves to every job it matches")
    check(resolve("id", "00000000-0000-4000-8000-000000000002", jobs) ==
          (["watchdog"], False), "a job id resolves to its job")

    def watch(command, sid=1):
        return Step("00000000-0000-4000-8000-000000000002", sid, "stop",
                    command=command)

    found = analyse_references(jobs, [watch(
        "IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs j WHERE j.name = "
        "N'nightly load') EXEC msdb.dbo.sp_stop_job @job_name = "
        "N'nightly load';")])
    check(kinds(found) == ["DISARMED"],
          "a watchdog whose target was renamed is DISARMED, once  "
          "<-- pinned defect")
    check(found[0].job == "watchdog" and "the job named 'nightly load'" in
          found[0].text and found[0].gate,
          "and it is reported against the watchdog, and fails the gate")
    found = analyse_references(jobs, [watch(
        "EXEC sp_stop_job @job_name = N'nightly load v2'")])
    check(kinds(found) == ["NAME_BOUND"] and not found[0].gate,
          "a name that resolves today is NAME_BOUND, a warning")
    found = analyse_references(jobs, [watch(
        "EXEC sp_stop_job @job_name = N'HOURLY LOAD'")])
    check("case-sensitive" in found[0].text,
          "a case-only match says a case-sensitive collation disarms it")
    found = analyse_references(jobs, [watch(
        "SELECT 1 FROM msdb.dbo.sysjobs WHERE name LIKE '%load%'")])
    check(kinds(found) == ["NAME_BOUND"] and
          "'Hourly Load', 'nightly load v2'" in found[0].text,
          "a LIKE that matches lists what it matches")
    found = analyse_references(jobs, [watch(
        "SELECT 1 FROM msdb.dbo.sysjobs WHERE name LIKE '%refresh%'")])
    check(kinds(found) == ["DISARMED"],
          "a LIKE that matches no job is DISARMED")
    found = analyse_references(jobs, [watch(
        "EXEC sp_start_job @job_id = '00000000-0000-4000-8000-000000000003'"
        "; EXEC sp_start_job @job_id = '00000000-0000-4000-8000-0000000000ff'"
    )])
    check(kinds(found) == ["DISARMED"] and "0000000000ff" in found[0].text,
          "a job id that exists is fine, and one that does not is DISARMED")

    # ---- history
    t0 = datetime.datetime(2026, 9, 1, 2, 0, 0)
    seq = [0]

    def run(seconds, status=1, day=0, steps=()):
        rows = []
        for sid, st in steps:
            seq[0] += 1
            rows.append(HistoryRow(seq[0], jid, sid, st,
                                   t0 + datetime.timedelta(days=day), 0))
        seq[0] += 1
        rows.append(HistoryRow(seq[0], jid, 0, status,
                               t0 + datetime.timedelta(days=day), seconds))
        return rows

    def history(*runs):
        out = []
        for r in runs:
            out.extend(r)
        return out

    hjob = [job()]
    normal = [run(100 + d, day=d) for d in range(6)]
    found, base = analyse_history(hjob, history(*normal))
    check(found == [] and base[jid] == (102.5, 6),
          "six runs of about 100 seconds are clean, with a median of 102.5")

    found, _ = analyse_history(hjob, history(*(normal + [run(18000, day=7)])))
    check(kinds(found) == ["FAR_PAST_NORMAL"],
          "a 5 hour run against a 102.5 second median is FAR_PAST_NORMAL  "
          "<-- pinned defect")
    check("reads as Succeeded" in found[0].text and "5h 00m 00s" in
          found[0].text and "176x" in found[0].text,
          "and it says the run reads as Succeeded, with its ratio")
    check("6 other successful run(s)" in found[0].text,
          "the long run is left out of its own median")

    found, _ = analyse_history(hjob, history(*(normal + [run(18000, 3,
                                                            day=7)])))
    check(kinds(found) == ["FAR_PAST_NORMAL"] and "reads as Canceled" in
          found[0].text,
          "a long run a watchdog cancelled is timed too")

    found, _ = analyse_history(hjob, history(*(normal + [run(299, day=7)])))
    check(found == [], "a run under --min-seconds is never far past normal")
    found, _ = analyse_history(hjob, history(*(normal + [run(1025, day=7)])))
    check(found == [], "exactly ten times the median is not past it")
    found, _ = analyse_history(hjob, history(*(normal + [run(1026, day=7)])))
    check(kinds(found) == ["FAR_PAST_NORMAL"], "one second more is")

    found, _ = analyse_history(
        hjob, history(*([run(0, day=d) for d in range(6)] +
                        [run(400, day=7)])))
    check(kinds(found) == ["FAR_PAST_NORMAL"] and "past the median 0s" in
          found[0].text, "a job that normally takes 0 seconds is timed too")

    found, _ = analyse_history(
        hjob, history(*(normal + [run(20000, day=7), run(20000, day=8)])))
    check(kinds(found) == ["FAR_PAST_NORMAL", "FAR_PAST_NORMAL"],
          "one hung run in the baseline does not hide the next one: a "
          "median, not a mean  <-- pinned defect")

    spiral = [run(100, day=d) for d in range(4)] + \
        [run(11000 + d, day=10 + d) for d in range(6)]
    found, _ = analyse_history(hjob, history(*spiral))
    check(found == [],
          "when most runs are long, the long run is the normal one")
    found, base = analyse_history(
        hjob, history(*spiral), min_runs=3,
        baseline_before=t0 + datetime.timedelta(days=5))
    check(kinds(found).count("FAR_PAST_NORMAL") == 6 and base[jid][1] == 4,
          "--baseline-before takes the median from before the spiral, and "
          "all six long runs show  <-- pinned defect")

    found, _ = analyse_history(hjob, history(*normal[:3]))
    check(kinds(found) == ["NO_BASELINE"] and "3 run(s) were not timed" in
          found[0].text, "three runs are too few to time, and say so")
    check(not found[0].gate, "and NO_BASELINE is a warning")

    found, _ = analyse_history(hjob, history(*(normal + [
        run(100, day=7, steps=[(1, 0), (2, 1)])])))
    check(kinds(found) == ["GREEN_OVER_FAILED_STEP"],
          "a run that reads as Succeeded with a failed step in it is "
          "GREEN_OVER_FAILED_STEP  <-- pinned defect")
    check("step 1 failed" in found[0].text, "and it names the failed step")
    found, _ = analyse_history(hjob, history(*(normal + [
        run(100, 0, day=7, steps=[(1, 0)])])))
    check(found == [], "a failed step in a failed run is an honest failure")
    found, _ = analyse_history(hjob, history(*(normal + [
        run(100, day=7, steps=[(1, 2), (1, 1)])])))
    check(found == [], "a retry that then succeeded is not a failed step")

    found, _ = analyse_history(hjob, history(*(normal + [
        run(90000, 4, day=7)])))
    check(found == [], "an In Progress outcome row is not timed")
    rows = history(*normal) + [HistoryRow(999, jid, 1, 0, t0, 0)]
    found, _ = analyse_history(hjob, rows)
    check(found == [], "step rows of a run with no outcome row yet are "
          "ignored")
    found, base = analyse_history(hjob, [])
    check(found == [] and base == {}, "a job with no history has no findings")

    # ---- activity
    exported = t0 + datetime.timedelta(days=9)
    _, base = analyse_history(hjob, history(*normal))

    def act(start_minutes_ago, stopped=False):
        start = exported - datetime.timedelta(minutes=start_minutes_ago)
        return Activity(jid, start, start if stopped else None, exported)

    found = analyse_activity(hjob, [act(90)], base)
    check(kinds(found) == ["RUNNING_PAST_NORMAL"] and "1h 30m 00s" in
          found[0].text, "a run going for 90 minutes against a 102.5 second "
          "median is RUNNING_PAST_NORMAL  <-- pinned defect")
    check(analyse_activity(hjob, [act(10)], base) == [],
          "a run going for 10 minutes is not")
    check(analyse_activity(hjob, [act(90, stopped=True)], base) == [],
          "a finished run in the activity export is ignored")
    check(analyse_activity(hjob, [Activity(jid, None, None, exported)],
                           base) == [],
          "a job not started in this Agent session is ignored")
    found = analyse_activity(hjob, [act(90)], {})
    check(kinds(found) == ["NO_BASELINE"],
          "a running job with no median is NO_BASELINE, never a pass")
    raises(lambda: analyse_activity(hjob, [act(-5)], base),
           "a start after the export time is refused")

    # ---- the tables together
    raises(lambda: check_tables([job(), job()], [], [], []),
           "a job id twice in the jobs export is refused")
    raises(lambda: check_tables([job()], [step(1), step(1)], [], []),
           "a step twice in the steps export is refused")
    raises(lambda: check_tables([job()], [Step(gid, 1, "x")], [], []),
           "a step of a job that is not in the jobs export is refused")
    raises(lambda: check_tables([job()], [], [HistoryRow(1, gid, 0, 1, t0, 1)],
                                []),
           "history of a job that is not in the jobs export is refused")

    report = build_report([job()], [step(1), step(2)])
    check(report.checked == ["step flow", "job references"] and
          gate(report) == 1, "a steps export alone checks flow and "
          "references, and fails on an unreachable step")
    report = build_report([job()], history=history(*normal),
                          activity=[act(90)])
    check(report.checked == ["run durations", "running jobs"] and
          kinds(report.findings) == ["RUNNING_PAST_NORMAL"],
          "a running job is timed against the history's median")
    report = build_report([job()], activity=[act(90)])
    check(kinds(report.findings) == ["NO_BASELINE"] and gate(report) == 0,
          "activity without history has no median, and does not pass a "
          "running job as normal")
    lines = describe(build_report([job()], [step(1, fail=QUIT_SUCCESS),
                                            step(2)]))
    check(lines[0] == "stepflow: 1 job(s), 2 step(s), 0 history row(s), 0 "
          "activity row(s)", "the report opens with the row counts")
    check(lines[3].startswith("FAIL  UNREACHABLE ") and
          lines[4].startswith("FAIL  FAIL_REPORTS_SUCCESS"),
          "findings print in kind order, worst first")
    check(lines[-1] == "VERDICT: 2 finding(s) fail the gate, 0 warning(s).",
          "the verdict counts failures and warnings")
    lines = describe(build_report([job()], [step(1)]))
    check(lines[-1] == "VERDICT: no finding fails the gate, 0 warning(s)." and
          lines[-2] == "", "a clean run says so")

    # ---- real files on disk, through main(). One temp directory, deleted
    # again at the end.
    tmp = tempfile.mkdtemp(prefix="stepflow-selftest-")

    def tmpfile(name, text, encoding="utf-8"):
        path = os.path.join(tmp, name)
        with open(path, "w", newline="", encoding=encoding) as handle:
            handle.write(text)
        return path

    def run_cli(argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = main(argv)
            except SystemExit as exc:
                code = exc.code
        return code, out.getvalue(), err.getvalue()

    # The shape the --sql queries produce: the header, then one line a row,
    # every name quoted, a command with commas, quotes and a CRLF inside it.
    j_load = "11111111-AAAA-4AAA-8AAA-111111111111"
    j_dog = "22222222-BBBB-4BBB-8BBB-222222222222"
    j_hour = "33333333-CCCC-4CCC-8CCC-333333333333"
    jobs_text = ((
        "job_id,name,start_step_id,eol\r\n"
        '%s,"nightly load v2",1,$\r\n'
        '%s,"watchdog, load",1,$\r\n'
        '%s,"hourly ""raw"" load",1,$\r\n') % (j_load, j_dog, j_hour))
    jobs_csv = tmpfile("jobs.csv", jobs_text)
    steps_text = ((
        "job_id,step_id,step_name,on_success_action,on_success_step_id,"
        "on_fail_action,on_fail_step_id,command,eol\r\n"
        '%s,1,"refresh",1,0,2,0,"EXEC dbo.refresh;",$\r\n'
        '%s,2,"rebuild helper",1,0,2,0,"EXEC dbo.rebuild;",$\r\n'
        '%s,1,"stop a hung load",1,0,2,0,"-- a, b, ""c""\r\n'
        "IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs AS j WHERE j.name = "
        "N'nightly load')\r\n"
        "  EXEC msdb.dbo.sp_stop_job @job_name = N'nightly load';\",$\r\n"
        '%s,1,"load",1,0,2,0,"SELECT 1",$\r\n') % (j_load, j_load, j_dog,
                                                   j_hour))
    steps_csv = tmpfile("steps.csv", steps_text)
    hist = ["instance_id,job_id,step_id,run_status,run_date,run_time,"
            "run_duration,eol"]
    for i in range(6):
        hist.append("%d,%s,1,1,2026090%d,20000,2,$" % (2 * i + 1, j_hour,
                                                       i + 1))
        hist.append("%d,%s,0,1,2026090%d,20000,2,$" % (2 * i + 2, j_hour,
                                                       i + 1))
    hist.append("13,%s,1,1,20260908,20000,1000,$" % j_hour)
    hist.append("14,%s,0,1,20260908,20000,1000,$" % j_hour)
    history_csv = tmpfile("history.csv", "\n".join(hist) + "\n")
    activity_csv = tmpfile("activity.csv", (
        "job_id,start_execution_date,stop_execution_date,exported_at,eol\n"
        "%s,2026-09-09 02:00:00,,2026-09-09 02:30:00,$\n"
        "%s,,,2026-09-09 02:30:00,$\n\n") % (j_hour, j_load))

    code, out, err = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                              "--history", history_csv, "--activity",
                              activity_csv])
    check(code == 1, "the full run exits 1, because findings fail the gate")
    check("FAIL  UNREACHABLE            nightly load v2: step 2 (rebuild "
          "helper) never runs" in out,
          "the appended step is named, under the job's own name")
    check("FAIL  DISARMED               watchdog, load: step 1 (stop a hung "
          "load) looks for the job named 'nightly load'" in out,
          "a job name with a comma survives the CSV, and the watchdog is "
          "DISARMED")
    check(out.count("DISARMED") == 1,
          "the WHERE and the sp_stop_job name the same job, so it is one "
          "finding")
    check("FAIL  FAR_PAST_NORMAL        hourly \"raw\" load: the run started "
          "2026-09-08 02:00:00 took 10m 00s, 300x the median 2s" in out,
          "a quoted job name survives, and its 10 minute run is 300x a "
          "2 second median")
    check("FAIL  RUNNING_PAST_NORMAL    hourly \"raw\" load: it has been "
          "running for 30m 00s" in out, "the running job is flagged")
    check("stepflow: 3 job(s), 4 step(s), 14 history row(s), 2 activity "
          "row(s)" in out, "every row is counted")
    check(err == "", "and nothing goes to stderr")

    code, out, _ = run_cli(["--jobs", jobs_csv, "--history", history_csv,
                            "--baseline-before", "2026-09-05",
                            "--min-runs", "3"])
    check(code == 1 and "of 4 other successful run(s)" in out,
          "--baseline-before limits the median to the earlier runs")

    clean_steps = tmpfile("clean.csv",
                          "job_id,step_id,step_name,on_success_action,"
                          "on_success_step_id,on_fail_action,on_fail_step_id,"
                          "command\n%s,1,load,1,0,2,0,SELECT 1\n" % j_hour)
    one_job = tmpfile("onejob.csv", "job_id,name,start_step_id\n%s,hourly,1\n"
                      % j_hour)
    code, out, _ = run_cli(["--jobs", one_job, "--steps", clean_steps])
    check(code == 0 and "VERDICT: no finding fails the gate" in out,
          "a clean export exits 0, and a file with no eol column is read")
    code, out, _ = run_cli(["--jobs", jobs_csv, "--steps", clean_steps])
    check(code == 1 and out.count("START_MISSING") == 2,
          "a job with no rows in the steps export is START_MISSING")

    utf16 = tmpfile("steps16.csv", steps_text, "utf-16")
    code16, out16, _ = run_cli(["--jobs", jobs_csv, "--steps", utf16])
    code8, out8, _ = run_cli(["--jobs", jobs_csv, "--steps", steps_csv])
    check(code16 == code8 and out16 == out8,
          "a UTF-16 export, which sqlcmd -u and a PowerShell 5.1 redirect "
          "write, reads the same as UTF-8  <-- pinned defect")
    bom = tmpfile("jobsbom.csv", jobs_text, "utf-8-sig")
    code, outb, _ = run_cli(["--jobs", bom, "--steps", steps_csv])
    check(outb == out8, "a UTF-8 byte order mark is stripped")

    path = tmpfile("latin.csv", "")
    with open(path, "wb") as handle:
        handle.write(b"job_id,name,start_step_id\n\xe9\xff\n")
    code, _, err = run_cli(["--jobs", path, "--steps", steps_csv])
    check(code == 2 and "neither UTF-8 nor UTF-16" in err and
          "-f o:65001" in err,
          "a file in a code page exits 2 and names -f o:65001")

    nocount = tmpfile("nocount.csv", jobs_text + "(3 rows affected)\r\n")
    code, out, err = run_cli(["--jobs", nocount, "--steps", steps_csv])
    check(code == 2 and "SET NOCOUNT ON" in err and out == "",
          "a '(3 rows affected)' line exits 2 and names SET NOCOUNT ON  "
          "<-- pinned defect")

    cut = tmpfile("cut.csv", (
        "job_id,step_id,step_name,on_success_action,on_success_step_id,"
        "on_fail_action,on_fail_step_id,command,eol\n"
        '%s,1,"refresh",3,0,2,0,"SELECT 1, 2; SELECT 1, 2; SEL\n'
        '%s,2,"two",1,0,2,0,"SELECT 2",$\n') % (j_load, j_load))
    code, out, err = run_cli(["--jobs", jobs_csv, "--steps", cut])
    check(code == 2 and "-y 0" in err and out == "",
          "a command cut at sqlcmd's 256 character default exits 2 and "
          "names -y 0  <-- pinned defect")
    check("cut.csv line 2:" in err,
          "and it names the line the cut row starts on, not where the "
          "reader stopped  <-- pinned defect")

    bad = tmpfile("badact.csv", "job_id,step_id,step_name,on_success_action,"
                  "on_success_step_id,on_fail_action,on_fail_step_id,command"
                  "\n%s,1,x,5,0,2,0,\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", bad])
    check(code == 2 and "on_success_action is 5" in err,
          "an action that is not 1 to 4 exits 2")
    bad = tmpfile("badint.csv", "job_id,step_id,step_name,on_success_action,"
                  "on_success_step_id,on_fail_action,on_fail_step_id,command"
                  "\n%s,one,x,1,0,2,0,\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", bad])
    check(code == 2 and "line 2" in err and "step_id is 'one'" in err,
          "a step_id that is not a number exits 2 and names the line")
    bad = tmpfile("badid.csv", "job_id,name,start_step_id\nnot-a-guid,x,1\n")
    code, _, err = run_cli(["--jobs", bad, "--steps", steps_csv])
    check(code == 2 and "'not-a-guid' is not a job id" in err and
          "NOCOUNT" not in err, "a job_id that is not a GUID exits 2")
    bad = tmpfile("nocol.csv", "job_id,title\n%s,x\n" % j_load)
    code, _, err = run_cli(["--jobs", bad, "--steps", steps_csv])
    check(code == 2 and "name, start_step_id" in err and
          "It has: job_id, title." in err and "-y 0" not in err,
          "a missing column exits 2 and lists the columns there are")
    bad = tmpfile("line.csv", "line\n-----\njob_id,name,start_step_id,eol\n")
    code, _, err = run_cli(["--jobs", bad, "--steps", steps_csv])
    check(code == 2 and "It has: line. Run sqlcmd with -y 0." in err,
          "the header sqlcmd prints without -y 0 exits 2 and names -y 0  "
          "<-- pinned defect")
    empty = tmpfile("empty.csv", "")
    code, _, err = run_cli(["--jobs", empty, "--steps", steps_csv])
    check(code == 2 and "(no columns at all)" in err,
          "an empty file exits 2 and says it has no columns")
    bad = tmpfile("badhist.csv", "instance_id,job_id,step_id,run_status,"
                  "run_date,run_time,run_duration\n1,%s,0,1,20260901,0,"
                  "199\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--history", bad])
    check(code == 2 and "line 2" in err and "HHMMSS" in err,
          "a malformed run_duration exits 2 and names the line")
    bad = tmpfile("badact2.csv", "job_id,start_execution_date,"
                  "stop_execution_date,exported_at\n%s,2026-09-09 02:00,,"
                  "2026-09-09 02:30:00\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--activity", bad])
    check(code == 2 and "YYYY-MM-DD HH:MM:SS" in err,
          "a date in another format exits 2")
    bad = tmpfile("noexp.csv", "job_id,start_execution_date,"
                  "stop_execution_date,exported_at\n%s,2026-09-09 02:00:00,"
                  "NULL,NULL\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--activity", bad])
    check(code == 2 and "exported_at is empty" in err,
          "an activity row with no export time exits 2, NULL read as empty")
    bad = tmpfile("future.csv", "job_id,start_execution_date,"
                  "stop_execution_date,exported_at\n%s,2026-09-09 03:00:00,,"
                  "2026-09-09 02:30:00\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--activity", bad])
    check(code == 2 and "after the export time" in err,
          "a run that starts after the export exits 2")
    orphan = tmpfile("orphan.csv", "job_id,step_id,step_name,"
                     "on_success_action,on_success_step_id,on_fail_action,"
                     "on_fail_step_id,command\n%s,1,x,1,0,2,0,\n" % gid)
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", orphan])
    check(code == 2 and "same time" in err,
          "exports taken at different times exit 2")
    big = tmpfile("big.csv", "job_id,name,start_step_id\n%s,%s,1\n"
                  % (j_load, "x" * 200000))
    code, _, err = run_cli(["--jobs", big, "--steps", steps_csv])
    check(code == 2 and err.startswith("error: "),
          "a field over the csv module's limit exits 2, not a traceback")
    code, _, err = run_cli(["--jobs", os.path.join(tmp, "missing.csv"),
                            "--steps", steps_csv])
    check(code == 2 and "missing.csv" in err, "a missing file exits 2")

    # ---- writing, and the parser
    report_path = os.path.join(tmp, "report.txt")
    code, out, _ = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                            "--report", report_path])
    check(code == 1 and not os.path.exists(report_path) and
          "nothing was written" in out,
          "--report without --apply writes nothing  <-- pinned defect")
    code, out, _ = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                            "--report", report_path, "--apply"])
    with open(report_path, encoding="utf-8") as handle:
        written = handle.read()
    check(code == 1 and written.startswith("stepflow: 3 job(s)") and
          "UNREACHABLE" in written and "wrote " in out,
          "--report with --apply writes the same report")
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                            "--report", tmp, "--apply"])
    check(code == 2 and "could not write" in err,
          "a report that cannot be written exits 2, not 1")
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                            "--report", report_path + "2", "--ap"])
    check(code == 64 and "--ap" in err and
          not os.path.exists(report_path + "2"),
          "a prefix --ap of --apply is refused, and nothing is written  "
          "<-- pinned defect")
    code, _, _ = run_cli(["--jobs", jobs_csv, "--step", steps_csv])
    check(code == 64, "a prefix --step of --steps is refused too")
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                            "--apply"])
    check(code == 64 and "--apply needs --report" in err,
          "--apply without --report is a usage error")
    code, _, err = run_cli(["--steps", steps_csv])
    check(code == 64 and "--jobs" in err, "--jobs is required")
    code, _, err = run_cli(["--jobs", jobs_csv])
    check(code == 64 and "--steps, --history or --activity" in err,
          "--jobs alone checks nothing, and says so")
    for flag, value in (("--factor", "1"), ("--min-runs", "0"),
                        ("--min-seconds", "-1"),
                        ("--baseline-before", "09/05/2026")):
        code, _, err = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                                flag, value])
        check(code == 64 and flag in err,
              "%s %s is a usage error" % (flag, value))
    for name in ("jobs", "steps", "history", "activity"):
        code, out, _ = run_cli(["--sql", name])
        check(code == 0 and out == SQL[name] and ",$')" in out and
              "SET NOCOUNT ON;" in out,
              "--sql %s prints its SELECT, with the eol column and "
              "NOCOUNT" % name)
    code, _, _ = run_cli(["--sql", "backups"])
    check(code == 64, "--sql with an unknown export is a usage error")
    argv = sys.argv
    sys.argv = ["stepflow.py", "--sql", "jobs"]
    try:
        code, out, _ = run_cli(None)
    finally:
        sys.argv = argv
    check(code == 0 and out == SQL["jobs"],
          "main with no argv reads the arguments after the program name")

    # ---- the harness itself
    mark = len(failed)
    quiet = sys.stdout
    sys.stdout = io.StringIO()
    try:
        check(False, "probe: a false condition must be recorded as a failure")
        raises(lambda: None, "probe: a call that raises nothing must fail")
        raises(lambda: [][0], "probe: the wrong exception must fail")
    finally:
        sys.stdout = quiet
    probe = failed[mark:]
    del failed[mark:]
    check(len(probe) == 3,
          "check() and raises() really do record a failure  <-- pinned defect")

    check(summary(2, ["x"]) == (["3 assertions, 1 failed", "  FAILED: x"], 1)
          and summary(2, []) == (["2 assertions, 0 failed"], 0),
          "the footer counts a failure and exits 1 on it")

    # The file must import as a module without running main. Bytecode is
    # not written, so the self-test leaves nothing behind.
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, here)
    dont_write = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        module = importlib.import_module("stepflow")
    finally:
        sys.dont_write_bytecode = dont_write
        sys.path.remove(here)
    check(module.SQL == SQL and callable(module.main),
          "the file imports as a module without running main")

    shutil.rmtree(tmp, ignore_errors=True)

    print("-" * 68)
    lines, code = summary(passed[0], failed)
    for line in lines:
        print(line)
    return code


def summary(passed, failed):
    """The self-test footer lines and its exit code."""
    lines = ["%d assertions, %d failed" % (passed + len(failed), len(failed))]
    lines.extend("  FAILED: %s" % label for label in failed)
    return lines, 1 if failed else 0


# ----------------------------------------------------------------------- cli

_GUID = re.compile(r"\A[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\Z", re.I)


def read_table(path, required):
    """The rows of one export as dicts with lower-case keys, and line numbers.

    Raises ValueError for a malformed export and OSError for a missing one.
    """
    with open(path, "rb") as handle:
        data = handle.read()
    try:
        if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
            text = data.decode("utf-16")
        else:
            text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ValueError("%s is neither UTF-8 nor UTF-16. Run sqlcmd with "
                         "-f o:65001." % path)
    reader = csv.reader(io.StringIO(text, newline=""))
    header = [h.strip().lower() for h in next(reader, [])]
    absent = [c for c in required if c not in header]
    if absent:
        # sqlcmd prints the column name "line" and a row of dashes over the
        # --sql output unless -y 0 is given, and -y 0 also stops the cut.
        hint = " Run sqlcmd with -y 0." if header == ["line"] else ""
        raise ValueError("%s: columns not in the export: %s. It has: %s.%s"
                         % (path, ", ".join(absent),
                            ", ".join(header) or "(no columns at all)",
                            hint))
    rows = []
    end = reader.line_num
    for cells in reader:
        # A record can span lines, so name the line it starts on.
        line, end = end + 1, reader.line_num
        if not cells:
            continue
        row = dict((h, "") for h in header)
        row.update(zip(header, cells))
        row["_line"] = line
        if "rows affected" in row["job_id"]:
            raise ValueError("%s line %d: %r is a row count. Put SET NOCOUNT "
                             "ON at the top of the query."
                             % (path, line, row["job_id"]))
        if "eol" in header and row["eol"].strip() != "$":
            raise ValueError("%s line %d: the row is cut short. Run sqlcmd "
                             "with -y 0, or it cuts every long value at 256 "
                             "characters." % (path, line))
        rows.append(row)
    return rows


def _cell_int(row, column, path):
    value = row.get(column, "").strip()
    try:
        return int(value)
    except ValueError:
        raise ValueError("%s line %d: %s is %r, not a whole number"
                         % (path, row["_line"], column, value))


def _cell_job(row, path):
    value = row["job_id"].strip()
    if not _GUID.match(value):
        raise ValueError("%s line %d: %r is not a job id"
                         % (path, row["_line"], value))
    return value


def _cell_time(row, column, path, required=False):
    value = row.get(column, "").strip()
    if value in ("", "NULL"):
        if required:
            raise ValueError("%s line %d: %s is empty"
                             % (path, row["_line"], column))
        return None
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise ValueError("%s line %d: %s is %r, not YYYY-MM-DD HH:MM:SS"
                         % (path, row["_line"], column, value))


def load_jobs(path):
    return [Job(_cell_job(r, path), r["name"],
                _cell_int(r, "start_step_id", path))
            for r in read_table(path, COLUMNS["jobs"])]


def load_steps(path):
    steps = []
    for r in read_table(path, COLUMNS["steps"]):
        for column in ("on_success_action", "on_fail_action"):
            if _cell_int(r, column, path) not in (1, 2, 3, 4):
                raise ValueError("%s line %d: %s is %s, and only 1 to 4 are "
                                 "defined" % (path, r["_line"], column,
                                              r[column]))
        steps.append(Step(_cell_job(r, path), _cell_int(r, "step_id", path),
                          r["step_name"],
                          _cell_int(r, "on_success_action", path),
                          _cell_int(r, "on_success_step_id", path),
                          _cell_int(r, "on_fail_action", path),
                          _cell_int(r, "on_fail_step_id", path),
                          r["command"]))
    return steps


def load_history(path):
    rows = []
    for r in read_table(path, COLUMNS["history"]):
        try:
            start = run_start(_cell_int(r, "run_date", path),
                              _cell_int(r, "run_time", path))
            seconds = decode_duration(_cell_int(r, "run_duration", path))
        except ValueError as exc:
            raise ValueError("%s line %d: %s" % (path, r["_line"], exc))
        rows.append(HistoryRow(_cell_int(r, "instance_id", path),
                               _cell_job(r, path),
                               _cell_int(r, "step_id", path),
                               _cell_int(r, "run_status", path),
                               start, seconds))
    return rows


def load_activity(path):
    return [Activity(_cell_job(r, path),
                     _cell_time(r, "start_execution_date", path),
                     _cell_time(r, "stop_execution_date", path),
                     _cell_time(r, "exported_at", path, required=True))
            for r in read_table(path, COLUMNS["activity"])]


class _Parser(argparse.ArgumentParser):
    """An argparse parser whose usage errors exit 64, not 2.

    2 is this tool's code for an export it could not read.
    """

    def error(self, message):
        self.print_usage(sys.stderr)
        print("%s: error: %s" % (self.prog, message), file=sys.stderr)
        sys.exit(64)


def _parse(argv):
    ap = _Parser(
        prog="stepflow.py",
        description="Flag SQL Server Agent job steps that never run, "
                    "watchdogs bound to a job name, and runs far past their "
                    "normal duration, from exported msdb rows.",
        epilog="Nothing is written without --apply.",
        allow_abbrev=False,
    )
    ap.add_argument("--jobs", help="the msdb.dbo.sysjobs export (--sql jobs)")
    ap.add_argument("--steps", help="the sysjobsteps export (--sql steps)")
    ap.add_argument("--history",
                    help="the sysjobhistory export (--sql history)")
    ap.add_argument("--activity",
                    help="the sysjobactivity export (--sql activity)")
    ap.add_argument("--factor", type=float, default=DEFAULT_FACTOR,
                    help="a run past this many times the median of the "
                         "job's other successful runs is flagged "
                         "(default %.0f)" % DEFAULT_FACTOR)
    ap.add_argument("--min-runs", dest="min_runs", type=int,
                    default=DEFAULT_MIN_RUNS,
                    help="the fewest successful runs a median is taken from "
                         "(default %d)" % DEFAULT_MIN_RUNS)
    ap.add_argument("--min-seconds", dest="min_seconds", type=float,
                    default=DEFAULT_MIN_SECONDS,
                    help="a run shorter than this is never flagged "
                         "(default %d)" % DEFAULT_MIN_SECONDS)
    ap.add_argument("--baseline-before", dest="baseline_before",
                    help="take the median only from runs that started "
                         "before this date, YYYY-MM-DD")
    ap.add_argument("--report", help="write the report to this file")
    ap.add_argument("--apply", action="store_true",
                    help="write --report. Without this nothing is written.")
    ap.add_argument("--sql", choices=sorted(SQL),
                    help="print the SELECT for one export and exit")
    ap.add_argument("--self-test", dest="self_test", action="store_true",
                    help="run the offline assertions and exit")
    return ap.parse_args(argv)


def _usage(message):
    print("error: %s" % message, file=sys.stderr)
    return 64


def main(argv=None):
    args = _parse(sys.argv[1:] if argv is None else argv)

    if args.self_test:
        return self_test()
    if args.sql:
        sys.stdout.write(SQL[args.sql])
        return 0
    if not args.jobs:
        return _usage("--jobs is required. Print its SELECT with --sql jobs.")
    if not (args.steps or args.history or args.activity):
        return _usage("give --steps, --history or --activity as well. "
                      "--jobs alone checks nothing.")
    if args.factor <= 1:
        return _usage("--factor must be more than 1, or a normal run is "
                      "flagged.")
    if args.min_runs < 1:
        return _usage("--min-runs must be 1 or more.")
    if args.min_seconds < 0:
        return _usage("--min-seconds cannot be negative.")
    if args.apply and not args.report:
        return _usage("--apply needs --report.")
    before = None
    if args.baseline_before:
        try:
            before = datetime.datetime.strptime(args.baseline_before,
                                                "%Y-%m-%d")
        except ValueError:
            return _usage("--baseline-before is %r, not YYYY-MM-DD."
                          % args.baseline_before)

    try:
        jobs = load_jobs(args.jobs)
        steps = load_steps(args.steps) if args.steps else None
        history = load_history(args.history) if args.history else None
        activity = load_activity(args.activity) if args.activity else None
        report = build_report(jobs, steps, history, activity, args.factor,
                              args.min_runs, args.min_seconds, before)
    except (ValueError, OSError, csv.Error) as exc:
        # csv.Error too: a field over csv's limit, or a NUL in the file, is
        # an export that could not be read, not a finding.
        print("error: %s" % exc, file=sys.stderr)
        return 2

    lines = describe(report)
    for line in lines:
        print(line)

    if args.report:
        if not args.apply:
            print("\n--report without --apply: nothing was written.")
        else:
            # A traceback here would exit 1, which reads as "findings". A
            # failed write is a different fact with its own code.
            try:
                with open(args.report, "w", encoding="utf-8") as handle:
                    handle.write("\n".join(lines) + "\n")
            except OSError as exc:
                print("error: could not write %s: %s" % (args.report, exc),
                      file=sys.stderr)
                return 2
            print("\nwrote %s" % args.report)
    return gate(report)


if __name__ == "__main__":
    sys.exit(main())
