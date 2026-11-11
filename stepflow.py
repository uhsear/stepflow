#!/usr/bin/env python
"""Flag SQL Server Agent job steps that never run, watchdogs that find their
job by name, runs far past their normal duration, and jobs that tell nobody
when they fail, from exported msdb rows.

A job had one step that quit reporting success. Someone appended a second
step with a script, and the first step still quit reporting success, so the
second step never ran. The job stayed green. A watchdog job looked its target
up by name and stopped it when it hung. The target was renamed, the watchdog
found nothing to stop, and it stayed green too. A run that took more than a
hundred times its normal duration went into the history as Succeeded. A job
had no operator set for a failure, and the Agent mail profile had been set with
a legacy parameter that stores nothing, so no failure ever sent mail.

SQL Server Management Studio warns about an unreachable step when the job is
edited in its dialog, but a T-SQL edit gets no warning. A community script
from 2012 finds steps that nothing routes to, one hop deep. dbachecks compares
a running job with its average duration over a live connection. This tool
reads exported msdb rows offline, walks the whole step graph from the start
step, resolves every job that a string literal in a step command names, and
times each finished or running run against the median of the job's other
successful runs. It also reads the job notification columns, the operators and
the Agent mail setting, and names each job that would tell nobody about a
failure.

    python stepflow.py --self-test
    python stepflow.py --sql steps > steps.sql
    python stepflow.py --jobs jobs.csv --steps steps.csv
    python stepflow.py --jobs jobs.csv --history history.csv --activity activity.csv
    python stepflow.py --jobs jobs.csv --notify notify.csv --operators operators.csv --mail mail.csv
    python stepflow.py --jobs jobs.csv --steps steps.csv --report out.txt --apply

Nothing is written without --apply, except that --self-test writes fixtures
to a temporary directory and removes it. Nothing connects to a server.

Exit codes: 0 no finding fails the gate and every check ran, 1 at least one
finding fails the gate, 2 an export could not be read or is malformed, 3 no
finding fails but a check could not be made (NO_BASELINE, MAIL_UNCHECKED),
64 usage error.
"""

from __future__ import print_function

import argparse
import atexit
import bisect
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
        "RUNNING_PAST_NORMAL", "NO_FAILURE_ALERT", "MAIL_NEVER_SENDS")
WARN = ("FAIL_CAN_END_GREEN", "NAME_BOUND", "NO_BASELINE", "MAIL_UNCHECKED")
KINDS = GATE + WARN
# Warnings for a check the run could not make. A clean run that holds one
# exits 3, not 0, so a pipeline cannot read "could not tell" as "all clear".
UNTOLD = ("NO_BASELINE", "MAIL_UNCHECKED")

# notify_level_* values (dbo.sysjobs): a bitmask, 1 on success and 2 on failure.
LEVEL_WORDS = {0: "never", 1: "on success only", 2: "on failure",
               3: "whenever the job completes"}

# The name the mail findings are filed under: they belong to the Agent.
AGENT = "(SQL Server Agent)"

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
    "notify": """SET NOCOUNT ON;
SELECT line FROM (
  SELECT 0 AS k, CAST(N'job_id,enabled,email_level,email_operator_id,page_level,page_operator_id,netsend_level,netsend_operator_id,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(CONVERT(char(36), j.job_id), N',', j.enabled, N',',
                   j.notify_level_email, N',', j.notify_email_operator_id, N',',
                   j.notify_level_page, N',', j.notify_page_operator_id, N',',
                   j.notify_level_netsend, N',', j.notify_netsend_operator_id,
                   N',$')
  FROM msdb.dbo.sysjobs AS j
) AS t ORDER BY k;
""",
    "operators": """SET NOCOUNT ON;
SELECT line FROM (
  SELECT 0 AS k, CAST(N'id,name,enabled,email_address,pager_address,pager_days,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(o.id, N',"', REPLACE(o.name, N'"', N'""'), N'",',
                   o.enabled, N',"',
                   REPLACE(ISNULL(o.email_address, N''), N'"', N'""'), N'","',
                   REPLACE(ISNULL(o.pager_address, N''), N'"', N'""'), N'",',
                   o.pager_days, N',$')
  FROM msdb.dbo.sysoperators AS o
) AS t ORDER BY k;
""",
    "mail": """SET NOCOUNT ON;
DECLARE @use int, @dbmail nvarchar(255), @found int, @xps int;
EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\\Microsoft\\MSSQLServer\\SQLServerAgent', N'UseDatabaseMail', @use OUTPUT, N'no_output';
EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\\Microsoft\\MSSQLServer\\SQLServerAgent', N'DatabaseMailProfile', @dbmail OUTPUT, N'no_output';
SELECT @found = COUNT(*) FROM msdb.dbo.sysmail_profile WHERE name = @dbmail;
SELECT @xps = CONVERT(int, value_in_use) FROM sys.configurations WHERE name = N'Database Mail XPs';
SELECT line FROM (
  SELECT 0 AS k, CAST(N'platform,use_databasemail,databasemail_profile,profile_exists,database_mail_xps,eol' AS nvarchar(max)) AS line
  UNION ALL
  SELECT 1, CONCAT(CASE WHEN @@VERSION LIKE N'%on Linux%' THEN N'Linux' ELSE N'Windows' END,
                   N',', ISNULL(@use, 0), N',"',
                   REPLACE(ISNULL(@dbmail, N''), N'"', N'""'), N'",',
                   CASE WHEN @found > 0 THEN 1 ELSE 0 END, N',',
                   ISNULL(@xps, 0), N',$')
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
    "notify": ("job_id", "enabled", "email_level", "email_operator_id",
               "page_level", "page_operator_id", "netsend_level",
               "netsend_operator_id"),
    "operators": ("id", "name", "enabled", "email_address", "pager_address",
                  "pager_days"),
    "mail": ("platform", "use_databasemail", "databasemail_profile"),
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


class Notify(object):
    """One job's notification settings. Each channel is (level, operator id)."""

    def __init__(self, job_id, enabled, email=(0, 0), page=(0, 0),
                 netsend=(0, 0)):
        self.job_id = job_id.lower()
        self.enabled = enabled
        self.channels = (("e-mail", email), ("pager", page),
                         ("net send", netsend))


class Operator(object):
    def __init__(self, op_id, name, enabled=1, email_address="",
                 pager_address="", pager_days=0):
        self.op_id = op_id
        self.name = name
        self.enabled = enabled
        self.email_address = email_address.strip()
        self.pager_address = pager_address.strip()
        self.pager_days = pager_days


class Mail(object):
    """The Agent mail setting as the registry holds it."""

    def __init__(self, platform, use_databasemail, databasemail_profile="",
                 profile_exists=None, database_mail_xps=None):
        self.platform = platform.strip()
        self.use_databasemail = use_databasemail
        self.databasemail_profile = databasemail_profile.strip()
        # None means the export did not say. 0 or 1 is what the SELECT read.
        self.profile_exists = profile_exists
        self.database_mail_xps = database_mail_xps


class Finding(object):
    def __init__(self, kind, job, text):
        self.kind = kind
        self.job = job
        self.text = text

    @property
    def gate(self):
        return self.kind in GATE


class Report(object):
    def __init__(self, findings, counts, checked, alert_counts=None):
        self.findings = findings
        self.counts = counts
        self.checked = checked
        # (notify rows, operators) when the alert exports were given.
        self.alert_counts = alert_counts

    @property
    def failing(self):
        return [f for f in self.findings if f.gate]

    @property
    def warnings(self):
        return [f for f in self.findings if not f.gate]

    @property
    def untold(self):
        return [f for f in self.findings if f.kind in UNTOLD]


# ------------------------------------------------------------- the decisions

def clip(value, width=32):
    """repr(value) cut to width characters, for an error message.

    A misaligned export puts other columns into the cell an error names, and
    that text must not be copied whole into a log.
    """
    text = repr(value)
    return text if len(text) <= width else text[:width - 4] + "...'"


_PLAIN_CELL = re.compile(r"\A[0-9 .:+-]{0,24}\Z")


def show_cell(value):
    """What an error may say about an export cell it rejected.

    A misaligned export puts a command, a password or an e-mail address into
    the cell an error names, so the text is shown only when it holds nothing
    but digits and the signs of a number or a time.
    """
    if _PLAIN_CELL.match(value):
        return repr(value)
    return "a value of %d characters" % len(value)


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
    except (ValueError, OverflowError):
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
        # A quit has no target, so the check below cannot fire for it.
        target = target_of(step, step.on_fail_action, step.on_fail_step_id)
        if target in by_id and can_end_green(by_id, target):
            findings.append(Finding(
                "FAIL_CAN_END_GREEN", job.name,
                "when step %d (%s) fails, the job goes on to step %d (%s), "
                "and a path from there quits reporting success."
                % (sid, step.name, target, by_id[target].name)))
    return findings


# Every string literal is cut out of the command first and replaced by a
# marker, \x01 and its number, and every comment is dropped. The patterns then
# run over the code only. A name inside a comment, or inside a string that is
# dynamic SQL or a PRINT, is not a binding. A literal that is followed by + is
# the start of a name built at run time, so it is not a name either.
_TOKEN = re.compile(r"'(?:[^']|'')*'|--[^\r\n]*|/\*")
_LIT = r"N?\x01(\d+)\x01(?!\s*\+)"
_NAME_ARG = re.compile(r"@job_name\s*=\s*" + _LIT, re.I)
_POSITIONAL = re.compile(r"\bsp_(?:start|stop)_job\s+" + _LIT, re.I)
_NAME_FILTER = re.compile(
    r"(?<![@\w.\]])((?:\[?\w+\]?\s*\.\s*)*)\[?name\]?\s*(=|like)\s*" + _LIT,
    re.I)
_ID_ARG = re.compile(r"@job_id\s*=\s*N?\x01(\d+)\x01", re.I)
_GUID_LIT = re.compile(r"\A\{?([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})\}?\Z",
                       re.I)
_SYSJOBS = re.compile(r"\[?\bsysjobs(?:_view)?\b\]?(?:\s+(?:as\s+)?(\w+))?", re.I)
_SOURCE = re.compile(r"\b(?:from|join)\s+(?:\[?\w+\]?\s*\.\s*)*\[?(\w+)", re.I)
_PROC = re.compile(r"\bsp_\w+", re.I)
_NOT_EXISTS = re.compile(r"\bnot\s+exists\s*\(", re.I)
_NOT_ALIAS = frozenset(("where", "join", "inner", "left", "right", "full",
                        "cross", "outer", "on", "with", "set", "group",
                        "order", "union", "having", "option", "for", "as"))


def _skeleton(text):
    """(code, literals): text without comments, with literals cut out."""
    text = text.replace("\x01", " ")
    out = []
    literals = []
    pos = 0
    while True:
        match = _TOKEN.search(text, pos)
        if match is None:
            out.append(text[pos:])
            return "".join(out), literals
        out.append(text[pos:match.start()])
        pos = match.end()
        token = match.group()
        if token[0] == "'":
            literals.append(token[1:-1])
            out.append("\x01%d\x01" % (len(literals) - 1))
        else:
            out.append(" ")
        if token == "/*":
            # Block comments nest in T-SQL. An unclosed one runs to the end.
            depth = 1
            while depth and pos < len(text):
                if text.startswith("/*", pos):
                    depth += 1
                    pos += 2
                elif text.startswith("*/", pos):
                    depth -= 1
                    pos += 2
                else:
                    pos += 1


def _inside_not_exists(code, pos):
    """True when pos is inside the parentheses of a NOT EXISTS (...)."""
    for match in _NOT_EXISTS.finditer(code, 0, pos):
        depth = 1
        for char in code[match.end():pos]:
            depth += (char == "(") - (char == ")")
            if depth == 0:
                break
        else:
            return True
    return False


def job_references(text):
    """The jobs a command names, as (kind, value) pairs in first-seen order.

    kind is "name", "like" or "id". A name compared in a WHERE clause counts
    only when the command reads msdb's sysjobs and the column is sysjobs's
    own, because "name" is the most common column name there is: j.name counts
    when j is an alias of sysjobs, and a bare name counts when sysjobs is the
    only table read. A value holding an Agent token such as
    $(ESCAPE_SQUOTE(JOBNAME)) is resolved at run time, so it is skipped. So is
    the name that sp_add_job gives a new job, and a name looked up inside a
    NOT EXISTS: both expect the job to be absent.
    """
    code, literals = _skeleton(text)
    found = []

    def add(kind, value):
        value = value.replace("''", "'")
        if "$(" in value:
            return
        if (kind, value) not in found:
            found.append((kind, value))

    for match in _NAME_ARG.finditer(code):
        procs = _PROC.findall(code, 0, match.start())
        if procs and procs[-1].lower() == "sp_add_job":
            continue
        add("name", literals[int(match.group(1))])
    for match in _POSITIONAL.finditer(code):
        add("name", literals[int(match.group(1))])
    tables = list(_SYSJOBS.finditer(code))
    if tables:
        names = set(("sysjobs", "sysjobs_view"))
        for table in tables:
            alias = (table.group(1) or "").lower()
            if alias and alias not in _NOT_ALIAS:
                names.add(alias)
        alone = all(m.group(1).lower() in ("sysjobs", "sysjobs_view")
                    for m in _SOURCE.finditer(code))
        for match in _NAME_FILTER.finditer(code):
            prefix = match.group(1).replace("[", "").replace("]", "")
            qualifier = prefix.rstrip(". ").split(".")[-1].strip().lower()
            if (qualifier in names if qualifier else alone) and \
                    not _inside_not_exists(code, match.start()):
                add("like" if match.group(2).lower() == "like" else "name",
                    literals[int(match.group(3))])
    for match in _ID_ARG.finditer(code):
        guid = _GUID_LIT.match(literals[int(match.group(1))])
        if guid:
            add("id", guid.group(1).lower())
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
    try:
        return re.compile("".join(out) + r"\Z", re.I | re.S)
    except re.error:
        # [z-a] and [^] are not classes. Exit 2 with the pattern, not a
        # traceback, which would exit 1 and read as a finding.
        raise ValueError("the LIKE pattern %s is not a valid character "
                         "class pattern" % clip(pattern))


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


def median_less(vals, skip):
    """The median of the sorted list vals without one occurrence of skip.

    skip None leaves vals whole. The same number as statistics.median of the
    shorter list, without building it: a job with many retained runs would
    otherwise cost a copy and a sort for every run.
    """
    cut = None if skip is None else bisect.bisect_left(vals, skip)

    def at(k):
        return vals[k + 1] if cut is not None and k >= cut else vals[k]

    count = len(vals) - (cut is not None)
    mid = count // 2
    return at(mid) if count % 2 else (at(mid - 1) + at(mid)) / 2


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
        pool_seconds = sorted(o.seconds for o in pool)
        pool_ids = set(o.instance_id for o in pool)
        if len(pool) >= min_runs:
            baselines[job.job_id] = (statistics.median(pool_seconds),
                                     len(pool))
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
            skip = outcome.seconds if outcome.instance_id in pool_ids \
                else None
            others = len(pool_seconds) - (skip is not None)
            if others < min_runs:
                untimed += 1
                continue
            normal = median_less(pool_seconds, skip)
            if outcome.seconds >= min_seconds and \
                    outcome.seconds > factor * normal:
                ratio = ("%.0fx" % (outcome.seconds / normal) if normal
                         else "past")
                findings.append(Finding(
                    "FAR_PAST_NORMAL", job.name,
                    "the run started %s took %s, %s the median %s of %d "
                    "other successful run(s), and reads as %s."
                    % (started, fmt_seconds(outcome.seconds), ratio,
                       fmt_seconds(normal), others,
                       RUN_STATUS.get(outcome.run_status, "status %d"
                                      % outcome.run_status))))
        if not runs:
            findings.append(Finding(
                "NO_BASELINE", job.name,
                "it has no finished run in the history export, so nothing "
                "was timed."))
        elif untimed:
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


def channel_gap(channel, level, op_id, operators):
    """(tells, why) for one notification channel of one job.

    tells is True when the channel notifies an operator on failure. why says
    what is wrong, and is empty when the channel was never set.
    """
    if (level, op_id) == (0, 0):
        return False, ""
    if channel == "net send":
        # sysoperators documents netsend_address as "Not supported".
        return False, ("net send is set, and SQL Server documents the net "
                       "send address of an operator as not supported")
    if level not in (2, 3):
        return False, "the %s notify level is '%s'%s" % (
            channel, LEVEL_WORDS[level],
            ", and no operator is set" if op_id == 0 else "")
    if op_id == 0:
        return False, "the %s notify level is '%s', and no operator is set" % (
            channel, LEVEL_WORDS[level])
    op = operators.get(op_id)
    if op is None:
        return False, ("%s notifies operator id %d, which is not in the "
                       "operators export" % (channel, op_id))
    if not op.enabled:
        return False, "%s notifies operator '%s', which is disabled" % (
            channel, op.name)
    if channel == "e-mail" and not op.email_address:
        return False, "%s notifies operator '%s', which has no e-mail address" \
            % (channel, op.name)
    if channel == "pager" and not op.pager_address:
        return False, "%s notifies operator '%s', which has no pager address" \
            % (channel, op.name)
    if channel == "pager" and not op.pager_days:
        return False, ("%s notifies operator '%s', which is never available "
                       "for pages (pager_days is 0)" % (channel, op.name))
    return True, ""


def analyse_alerts(jobs, notify, operators):
    """NO_FAILURE_ALERT for each enabled job no operator hears a failure of."""
    names = dict((j.job_id, j.name) for j in jobs)
    by_id = dict((o.op_id, o) for o in operators)
    findings = []
    for row in notify:
        if not row.enabled:
            continue
        results = [channel_gap(channel, level, op_id, by_id)
                   for channel, (level, op_id) in row.channels]
        if any(tells for tells, _ in results):
            continue
        why = [text for _, text in results if text]
        findings.append(Finding(
            "NO_FAILURE_ALERT", names[row.job_id],
            "no operator is told when this job fails: %s." %
            ("; ".join(why) if why else "no notification channel is set")))
    return findings


def analyse_mail(mail):
    """MAIL_NEVER_SENDS or MAIL_UNCHECKED for the Agent mail setting."""
    if mail.platform.lower() == "linux":
        return [Finding(
            "MAIL_UNCHECKED", AGENT,
            "this export came from SQL Server on Linux. There the Agent mail "
            "profile is set in mssql.conf (sqlagent.databasemailprofile), and "
            "the export cannot read it. Check that setting by hand.")]
    if mail.use_databasemail != 1:
        return [Finding(
            "MAIL_NEVER_SENDS", AGENT,
            "Agent mail is not enabled: use_databasemail is %d%s. Agent sends "
            "no job or alert notification. A profile set with the "
            "@email_profile parameter of sp_set_sqlagent_properties is "
            "stored nowhere: that parameter is obsolete, and SQL Mail was "
            "discontinued in SQL Server 2012. Set @use_databasemail = 1 and "
            "@databasemail_profile, then restart Agent."
            % (mail.use_databasemail,
               ", and the profile '%s' is set" % mail.databasemail_profile
               if mail.databasemail_profile else ""))]
    if not mail.databasemail_profile:
        return [Finding(
            "MAIL_NEVER_SENDS", AGENT,
            "Database Mail is selected for Agent and no mail profile is "
            "set. Agent sends no job or alert notification. Set "
            "@databasemail_profile, then restart Agent.")]
    if mail.profile_exists is None or mail.database_mail_xps is None:
        return [Finding(
            "MAIL_UNCHECKED", AGENT,
            "this export does not say whether the profile '%s' exists in "
            "msdb or whether the 'Database Mail XPs' option is on. Agent "
            "sends nothing if either is missing. Print the mail SELECT "
            "again with --sql mail, or check both by hand."
            % mail.databasemail_profile)]
    findings = []
    if not mail.profile_exists:
        findings.append(Finding(
            "MAIL_NEVER_SENDS", AGENT,
            "the Agent mail profile '%s' is not in "
            "msdb.dbo.sysmail_profile. Agent sends no job or alert "
            "notification. Check the spelling, or create the profile."
            % mail.databasemail_profile))
    if not mail.database_mail_xps:
        findings.append(Finding(
            "MAIL_NEVER_SENDS", AGENT,
            "the 'Database Mail XPs' option is 0. Database Mail is not "
            "available, so Agent sends no job or alert notification. Turn "
            "the option on with sp_configure."))
    return findings


def check_tables(jobs, steps, history, activity, notify=(), operators=()):
    """Refuse exports that disagree with each other. Raises ValueError."""
    ids = set()
    for job in jobs:
        if job.job_id in ids:
            raise ValueError("job id %s is in the jobs export twice"
                             % job.job_id)
        ids.add(job.job_id)
    keys = set()
    for name, rows in (("steps", steps), ("history", history),
                       ("activity", activity), ("notify", notify)):
        for row in rows:
            if row.job_id not in ids:
                raise ValueError(
                    "the %s export names job id %s, which is not in the jobs "
                    "export. Take every export at the same time. msdb also "
                    "keeps the history rows of a job deleted with "
                    "@delete_history = 0." % (name, row.job_id))
    for step in steps:
        key = (step.job_id, step.step_id)
        if key in keys:
            raise ValueError("job id %s has step %d twice in the steps "
                             "export" % key)
        keys.add(key)
    seen = set()
    for row in notify:
        if row.job_id in seen:
            raise ValueError("job id %s is in the notify export twice"
                             % row.job_id)
        seen.add(row.job_id)
    if notify and seen != ids:
        raise ValueError(
            "the notify export has no row for job id %s. Take every export "
            "at the same time, so a job is not judged on notification "
            "settings that were never read." % sorted(ids - seen)[0])
    op_ids = set()
    for op in operators:
        if op.op_id in op_ids:
            raise ValueError("operator id %d is in the operators export "
                             "twice" % op.op_id)
        op_ids.add(op.op_id)


def build_report(jobs, steps=None, history=None, activity=None,
                 factor=DEFAULT_FACTOR, min_runs=DEFAULT_MIN_RUNS,
                 min_seconds=DEFAULT_MIN_SECONDS, baseline_before=None,
                 notify=None, operators=None, mail=None):
    """Run every check the supplied exports allow. None means not supplied."""
    if not jobs:
        raise ValueError("the jobs export has no rows, so nothing was "
                         "checked. Check the server, the database (msdb) "
                         "and the permissions of the login that took it.")
    if activity is not None and not activity:
        raise ValueError("the activity export has no rows, so no running "
                         "job could be seen. Check that SQL Server Agent is "
                         "running and that the export read the right server.")
    if (notify is None) != (operators is None):
        raise ValueError("the notify and operators exports are read "
                         "together: an operator id means nothing without "
                         "the operators.")
    if notify is not None and not notify:
        raise ValueError("the notify export has no rows, so no job was "
                         "checked. Check the server, the database (msdb) "
                         "and the permissions of the login that took it.")
    check_tables(jobs, steps or [], history or [], activity or [],
                 notify or [], operators or [])
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
    alert_counts = None
    if notify is not None:
        checked.append("failure alerts")
        findings.extend(analyse_alerts(jobs, notify, operators))
        alert_counts = (len(notify), len(operators))
    if mail is not None:
        checked.append("agent mail")
        findings.extend(analyse_mail(mail))
    elif notify is not None:
        findings.append(Finding(
            "MAIL_UNCHECKED", AGENT,
            "no --mail export was given, so a job that names an operator may "
            "still send nothing: Agent sends no notification when its mail "
            "profile is not set."))
    findings.sort(key=lambda f: (KINDS.index(f.kind), f.job.lower(), f.text))
    counts = (len(jobs), len(steps or []), len(history or []),
              len(activity or []))
    return Report(findings, counts, checked, alert_counts)


def describe(report):
    head = ("stepflow: %d job(s), %d step(s), %d history row(s), %d activity "
            "row(s)" % report.counts)
    if report.alert_counts is not None:
        head += ", %d notify row(s), %d operator(s)" % report.alert_counts
    lines = [head, "checked: %s" % ", ".join(report.checked), ""]
    for f in report.findings:
        lines.append("%s  %-22s %s: %s" % ("FAIL" if f.gate else "WARN",
                                           f.kind, f.job, f.text))
    if report.findings:
        lines.append("")
    if report.failing:
        lines.append("VERDICT: %d finding(s) fail the gate, %d warning(s)."
                     % (len(report.failing), len(report.warnings)))
    elif report.untold:
        lines.append("VERDICT: no finding fails the gate, %d warning(s), but "
                     "%d check(s) could not be made, so this is not a clean "
                     "run." % (len(report.warnings), len(report.untold)))
    else:
        lines.append("VERDICT: no finding fails the gate, %d warning(s)."
                     % len(report.warnings))
    return lines


def gate(report):
    if report.failing:
        return 1
    return 3 if report.untold else 0


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
    check(decode_duration(5959) == 3599,
          "run_duration 5959 is 59m 59s: the top of both fields is valid")
    raises(lambda: run_start(99999999999999999999, 0),
           "a run_date past a C long is refused, not an OverflowError  "
           "<-- pinned defect")
    check(clip("ab") == "'ab'" and len(clip("x" * 100)) == 32 and
          clip("x" * 100).endswith("...'"),
          "clip() leaves a short value alone and cuts a long one to 32")

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

    found = analyse_flow(job(start=3), [step(2), step(1)])
    check(kinds(found) == ["START_MISSING"] and "has steps 1, 2." in
          found[0].text,
          "the steps a missing start step leaves are listed in order")
    found = analyse_flow(job(), [step(1), step(3), step(2)])
    check([f.text.split()[1] for f in found] == ["2", "3"],
          "unreachable steps are reported in step order, whatever the "
          "export order")

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

    check(job_references(
        "-- retired: EXEC msdb.dbo.sp_stop_job @job_name = N'old nightly';\n"
        "SELECT 1") == [],
          "a name in a -- comment is not a binding  <-- pinned defect")
    check(job_references("/* EXEC sp_stop_job N'old'; */ SELECT 1") == [],
          "a name in a block comment is not a binding  <-- pinned defect")
    check(job_references("/* a /* b */ sp_stop_job N'old' */ "
                         "EXEC sp_stop_job N'live'") == [("name", "live")],
          "block comments nest, as they do in T-SQL")
    check(job_references("EXEC sp_stop_job N'a' /* never closed "
                         "sp_stop_job N'b'") == [("name", "a")],
          "a block comment that is never closed runs to the end")
    check(job_references("-- it's here\nEXEC sp_stop_job N'x'") ==
          [("name", "x")], "an apostrophe in a comment opens no string")
    check(job_references("EXEC sp_stop_job N'a--b /* c'") ==
          [("name", "a--b /* c")], "comment marks inside a string stay text")
    check(job_references("EXEC sp_stop_job N'abc") == [],
          "a string that is never closed is not a name")
    check(job_references("EXEC sp_stop_job N'a\x01b'") == [("name", "a b")],
          "a control character in a command cannot fake a literal marker")
    check(job_references(
        "EXEC msdb.dbo.sp_stop_job @job_name = N'Backup - ' + @@SERVERNAME"
    ) == [], "a name built by concatenation is not read as its first part  "
          "<-- pinned defect")
    check(job_references("EXEC sp_stop_job @job_name = N'a' + N' b'") == [],
          "two literals joined by + are not a name either")
    check(job_references("EXEC sp_stop_job @job_name = N'ok'; SELECT 1 + 2")
          == [("name", "ok")], "a + after the statement is not concatenation")
    check(job_references(
        "DECLARE @sql nvarchar(max) = N'EXEC msdb.dbo.sp_stop_job "
        "@job_name = N''nightly'';'; EXEC sp_executesql @sql;") == [],
          "a call inside dynamic SQL is not seen, and no empty name is read  "
          "<-- pinned defect")
    check(job_references("PRINT 'call sp_stop_job N''x'' later'") == [],
          "a call written inside a string is not a binding")
    check(job_references(
        "SELECT j.name FROM msdb.dbo.sysjobs j JOIN msdb.dbo.syscategories "
        "c ON c.category_id = j.category_id WHERE c.name = N'Data "
        "Collector'") == [],
          "a name compared on another table is not a job name  "
          "<-- pinned defect")
    check(job_references(
        "SELECT j.name FROM msdb.dbo.sysjobs j JOIN msdb.dbo.syscategories "
        "c ON c.category_id = j.category_id WHERE j.name = N'x'") ==
          [("name", "x")], "and the alias of sysjobs is still read")
    check(job_references("SELECT 1 FROM [msdb].[dbo].[sysjobs] AS j WHERE "
                         "j.[name] = N'x'") == [("name", "x")],
          "brackets around the table and the column are read")
    check(job_references("SELECT 1 FROM msdb.dbo.sysjobs WHERE "
                         "msdb.dbo.sysjobs.name = N'x'") == [("name", "x")],
          "a name qualified by the full table name is read")
    check(job_references(
        "SELECT 1 FROM msdb.dbo.sysjobs; SELECT d.name FROM sys.databases "
        "AS d WHERE d.name = N'StagingDB'") == [],
          "a database name beside a sysjobs read is not a job name  "
          "<-- pinned defect")
    check(job_references(
        "SELECT 1 FROM msdb.dbo.sysjobs JOIN msdb.dbo.sysjobschedules ON "
        "1 = 1 WHERE name = N'x'") == [],
          "a bare name is read only when sysjobs is the only table")
    check(job_references("SELECT 1 FROM msdb.dbo.sysjobs; SET @name = N'x'")
          == [], "a variable called @name is not the column")
    check(job_references(
        "IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'X') "
        "EXEC msdb.dbo.sp_add_job @job_name = N'X'") == [],
          "a job that the step creates if it is missing is not a binding  "
          "<-- pinned defect")
    check(job_references("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs "
                         "WHERE (name = N'X'))") == [],
          "a NOT EXISTS stays open across nested parentheses")
    check(job_references("IF NOT EXISTS (SELECT 1 FROM dbo.t) SELECT 1 FROM "
                         "msdb.dbo.sysjobs j WHERE j.name = N'X'") ==
          [("name", "X")], "after the NOT EXISTS closes, a name is read")
    check(job_references("EXEC sp_add_job @job_name = N'a'; EXEC "
                         "sp_stop_job @job_name = N'b'") == [("name", "b")],
          "sp_add_job exempts only its own name")
    check(job_references("EXEC x @job_name = N'q'") == [("name", "q")],
          "a @job_name with no procedure before it is read")
    check(job_references("EXEC sp_start_job @job_id = 'nope'") == [],
          "a @job_id literal that is not a job id is not read")
    check(job_references("EXEC SP_ADD_JOB @job_name = N'a'") == [],
          "sp_add_job is found ignoring case")
    check(job_references("SELECT 1 FROM MSDB.DBO.SYSJOBS AS J WHERE "
                         "j.NAME = N'x'") == [("name", "x")],
          "table, alias and column are matched ignoring case")
    check(job_references("SELECT 1 FROM sysjobs j WHERE J.name = N'x'") ==
          [("name", "x")], "an alias is matched ignoring case")
    check(job_references("SELECT 1 FROM SYSJOBS_VIEW WHERE name = N'x'") ==
          [("name", "x")], "a bare name is read from an upper-case table")

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

    raises(lambda: like_regex("x[^]"),
           "a LIKE pattern with an empty negated class is refused  "
           "<-- pinned defect")
    raises(lambda: like_regex("a[z-a]"),
           "a LIKE pattern with a reversed range is refused  "
           "<-- pinned defect")

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
    check(resolve("name", " nightly load v2", jobs) == ([], False),
          "leading spaces are part of a name, and only trailing ones are "
          "ignored")
    lead = [Job("00000000-0000-4000-8000-0000000000c1", "  lead", 1),
            Job("00000000-0000-4000-8000-0000000000c2", "  FOLD", 1)]
    check(resolve("name", "lead", lead) == ([], False) and
          resolve("name", "fold", lead) == ([], False),
          "a job name with leading spaces is not the name without them")


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
    check(kinds(found) == ["NO_BASELINE"] and base == {} and
          "no finished run" in found[0].text and not found[0].gate,
          "a job with no run in a supplied history export is NO_BASELINE, "
          "never a silent pass  <-- pinned defect")
    found, _ = analyse_history(hjob, history(run(90000, 4)))
    check(kinds(found) == ["NO_BASELINE"],
          "a job whose only run is still In Progress is NO_BASELINE too")

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

    # ---- the boundaries of the timing rules
    found, base = analyse_history(
        hjob, history(*normal), min_runs=3,
        baseline_before=t0 + datetime.timedelta(days=3))
    check(base[jid][1] == 3,
          "a run that started exactly at --baseline-before is left out of "
          "the median")
    found, base = analyse_history(hjob, history(*normal), min_runs=6)
    check(base[jid][1] == 6,
          "exactly --min-runs runs are enough for a median")
    found, _ = analyse_history(hjob, history(*(normal[:3] + [
        run(20000, day=7)])))
    check(kinds(found) == ["NO_BASELINE"] and "4 run(s) were not timed" in
          found[0].text,
          "a hung run of a job with too few other runs is NO_BASELINE, not "
          "timed against a thin median")
    quick = [run(10, day=d) for d in range(6)]
    found, _ = analyse_history(hjob, history(*(quick + [run(300, day=7)])))
    check(kinds(found) == ["FAR_PAST_NORMAL"],
          "a run of exactly --min-seconds is timed")
    found, _ = analyse_history(hjob, history(*[run(1, day=d)
                                                for d in range(7)]),
                               min_seconds=0)
    check(found == [], "--min-seconds 0 does not flag a run at its median")

    def sample(seconds):
        return Activity(jid, exported - datetime.timedelta(seconds=seconds),
                        None, exported)

    base = {jid: (100, 6)}
    check(analyse_activity(hjob, [sample(1000)], base) == [],
          "a run going for exactly ten times its median is not past it")
    check(kinds(analyse_activity(hjob, [sample(1001)], base)) ==
          ["RUNNING_PAST_NORMAL"], "one second more is")
    base = {jid: (1, 6)}
    check(kinds(analyse_activity(hjob, [sample(300)], base)) ==
          ["RUNNING_PAST_NORMAL"],
          "a run going for exactly --min-seconds is timed")
    check(analyse_activity(hjob, [sample(299)], base) == [],
          "and one second under is not")
    check(analyse_activity(hjob, [sample(0)], base) == [],
          "a job started in the second of the export is not an error")

    def brute(vals, skip):
        rest = list(vals)
        if skip is not None:
            rest.remove(skip)
        return statistics.median(rest)

    lists = [[5], [1, 1], [1, 2, 3], [1, 2, 2, 9], [3, 3, 3, 3, 3],
             [1, 4, 4, 6, 10, 10, 11]]
    check(all(median_less(v, k) == brute(v, k)
              for v in lists for k in [None] + v if len(v) > 1 or k is None),
          "median_less() equals the median of the shorter list, for every "
          "list and every skipped value")

    jobs2 = [Job("00000000-0000-4000-8000-0000000000b1", "B job", 1),
             Job("00000000-0000-4000-8000-0000000000a1", "a job", 1)]
    check([f.job for f in build_report(jobs2, []).findings] ==
          ["a job", "B job"],
          "findings of one kind are ordered by job name, ignoring case")

    check(clip("x" * 30) == "'" + "x" * 30 + "'",
          "a value that fits in the width is not cut")
    mixed = [run(s, day=d) for d, s in enumerate((900, 100, 500, 200, 800,
                                                   300))]
    found, _ = analyse_history(hjob, history(*(mixed + [run(20000, day=7)])))
    check(kinds(found) == ["FAR_PAST_NORMAL"] and "median 6m 40s" in
          found[0].text,
          "the median is taken from durations in any order")

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
    check(kinds(report.findings) == ["NO_BASELINE"] and gate(report) == 3,
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

    # ---- failure alerts: one channel
    ops = [Operator(1, "ops", 1, "ops@example.invalid"),
           Operator(2, "off duty", 0, "off@example.invalid"),
           Operator(3, "blank", 1, "  "),
           Operator(4, "pager never", 1, "", "pg@example.invalid", 0),
           Operator(5, "pager ok", 1, "", "pg@example.invalid", 127),
           Operator(6, "pager blank", 1, "", "", 127)]
    by_op = dict((o.op_id, o) for o in ops)

    def gap(channel, level, op_id):
        return channel_gap(channel, level, op_id, by_op)

    check(gap("e-mail", 2, 1) == (True, "") and gap("e-mail", 3, 1)[0],
          "an operator with an address, told on failure or on completion, "
          "hears the failure")
    check(gap("e-mail", 0, 0) == (False, ""),
          "a channel that was never set says nothing")
    ok, why = gap("e-mail", 1, 1)
    check(not ok and why == "the e-mail notify level is 'on success only'",
          "a level of 1 tells the operator about success, not failure")
    check("'never'" in gap("e-mail", 0, 1)[1],
          "an operator with level 0 is never told")
    check(gap("e-mail", 1, 0)[1] == "the e-mail notify level is 'on success "
          "only', and no operator is set",
          "a level that is too low and no operator say both")
    check(gap("e-mail", 2, 0)[1] == "the e-mail notify level is 'on "
          "failure', and no operator is set",
          "a failure level with operator id 0 names the missing operator")
    check("operator id 9, which is not in the operators export" in
          gap("e-mail", 2, 9)[1], "an operator id that is not in sysoperators "
          "is named")
    check("'off duty', which is disabled" in gap("e-mail", 2, 2)[1],
          "a disabled operator hears nothing")
    check("which has no e-mail address" in gap("e-mail", 2, 3)[1],
          "an operator whose address is blank hears nothing")
    check(gap("pager", 2, 5) == (True, ""),
          "a pager operator with an address and pager days hears the failure")
    check("no pager address" in gap("pager", 2, 6)[1] and
          "no pager address" in gap("pager", 2, 1)[1],
          "a pager channel needs a pager address, not an e-mail address")
    check("never available for pages (pager_days is 0)" in
          gap("pager", 2, 4)[1],
          "an operator with pager_days 0 is never paged")
    check(not gap("net send", 2, 1)[0] and "not supported" in
          gap("net send", 2, 1)[1] and not gap("net send", 3, 5)[0],
          "net send never counts: sysoperators documents it as not "
          "supported")

    # ---- failure alerts: one job
    ja, jb, jc, jd = ("0a0a0a0a-0000-4000-8000-00000000000%d" % i
                      for i in (1, 2, 3, 4))
    four = [Job(ja, "healthy", 1), Job(jb, "success only", 1),
            Job(jc, "nothing set", 1), Job(jd, "retired", 1)]
    found = analyse_alerts(four, [
        Notify(ja, 1, email=(2, 1)),
        Notify(jb, 1, email=(1, 1), page=(2, 4)),
        Notify(jc, 1),
        Notify(jd, 0)], ops)
    check([f.job for f in found] == ["success only", "nothing set"] and
          kinds(found) == ["NO_FAILURE_ALERT"] * 2 and found[0].gate,
          "a job nobody hears a failure of is NO_FAILURE_ALERT, and it "
          "fails the gate  <-- pinned defect")
    check("the e-mail notify level is 'on success only'; pager notifies "
          "operator 'pager never'" in found[0].text and
          found[0].text.count("; ") == 1,
          "a job with two broken channels names both, once each")
    check(found[1].text == "no operator is told when this job fails: no "
          "notification channel is set.",
          "a job with no channel set says so")
    check(not any(f.job == "retired" for f in found),
          "a disabled job needs no alert, so it is not flagged")
    check(analyse_alerts(four[:1], [Notify(ja, 1, email=(2, 99),
                                           page=(3, 5))], ops) == [],
          "one working channel is enough, whatever the others say")

    # ---- failure alerts: the Agent mail setting
    def mail_kinds(*args):
        return kinds(analyse_mail(Mail(*args)))

    check(mail_kinds("Windows", 1, "Ops mail", 1, 1) == [],
          "database mail selected with a profile that exists and the XPs "
          "on has no finding")
    found = analyse_mail(Mail("Windows", 1, "Ops mail"))
    check(kinds(found) == ["MAIL_UNCHECKED"] and not found[0].gate and
          "'Ops mail'" in found[0].text and
          "'Database Mail XPs'" in found[0].text,
          "a profile name alone is not clean: an export that does not say "
          "whether the profile exists or the XPs are on is MAIL_UNCHECKED  "
          "<-- pinned defect")
    check(mail_kinds("Windows", 1, "Ops mail", 1, None) ==
          ["MAIL_UNCHECKED"] and
          mail_kinds("Windows", 1, "Ops mail", None, 1) ==
          ["MAIL_UNCHECKED"],
          "one unknown check is enough to leave the verdict unchecked")
    found = analyse_mail(Mail("Windows", 1, "no such profile", 0, 1))
    check(kinds(found) == ["MAIL_NEVER_SENDS"] and found[0].gate and
          "'no such profile' is not in msdb.dbo.sysmail_profile"
          in found[0].text,
          "a profile name that is not in msdb never sends  "
          "<-- pinned defect")
    found = analyse_mail(Mail("Windows", 1, "Ops mail", 1, 0))
    check(kinds(found) == ["MAIL_NEVER_SENDS"] and found[0].gate and
          "'Database Mail XPs' option is 0" in found[0].text,
          "Database Mail XPs off never sends, whatever the profile  "
          "<-- pinned defect")
    check(mail_kinds("Windows", 1, "gone", 0, 0) ==
          ["MAIL_NEVER_SENDS", "MAIL_NEVER_SENDS"],
          "a missing profile and XPs off are both named")
    found = analyse_mail(Mail("Windows", 0, ""))
    check(kinds(found) == ["MAIL_NEVER_SENDS"] and found[0].gate and
          found[0].job == AGENT and "Agent mail is not enabled: "
          "use_databasemail is 0" in found[0].text and
          "@email_profile" in found[0].text and "obsolete" in found[0].text
          and "discontinued in SQL Server 2012" in found[0].text,
          "Agent mail that was never enabled never sends, and the finding "
          "names the obsolete @email_profile parameter  <-- pinned defect")
    check(mail_kinds("Windows", 0, "Ops mail") == ["MAIL_NEVER_SENDS"],
          "a Database Mail profile with use_databasemail 0 is not used")
    found = analyse_mail(Mail("Windows", 1, "  "))
    check(kinds(found) == ["MAIL_NEVER_SENDS"] and
          "no mail profile is set" in found[0].text,
          "Database Mail selected with no profile never sends")
    found = analyse_mail(Mail("Linux", 1, ""))
    check(kinds(found) == ["MAIL_UNCHECKED"] and not found[0].gate and
          "mssql.conf" in found[0].text,
          "a Linux export is MAIL_UNCHECKED, not a false MAIL_NEVER_SENDS: "
          "the registry does not hold the Linux profile  <-- pinned defect")
    check(mail_kinds(" LINUX ", 0, "") == ["MAIL_UNCHECKED"] and
          mail_kinds(" windows ", 1, "p", 1, 1) == [],
          "the platform is read ignoring case and spaces")

    # ---- failure alerts: the tables together
    raises(lambda: check_tables(four, [], [], [], [Notify(ja, 1)] * 2),
           "a job twice in the notify export is refused")
    raises(lambda: check_tables(four, [], [], [], [Notify(ja, 1)]),
           "a job with no row in the notify export is refused, not judged "
           "on settings that were never read  <-- pinned defect")
    raises(lambda: check_tables(four[:1], [], [], [],
                                [Notify(ja, 1), Notify(jb, 1)]),
           "a notify row of a job that is not in the jobs export is refused")
    raises(lambda: check_tables(four, [], [], [], (), [Operator(1, "a"),
                                                       Operator(1, "b")]),
           "an operator id twice in the operators export is refused")
    all_rows = [Notify(j.job_id, 1, email=(2, 1)) for j in four]
    check(check_tables(four, [], [], [], all_rows, ops) is None,
          "a notify row for every job, and unique operators, is accepted")
    raises(lambda: build_report(four, notify=all_rows),
           "--notify without the operators is refused")
    raises(lambda: build_report(four, operators=ops),
           "--operators without the notify export is refused")
    raises(lambda: build_report(four, notify=[], operators=ops),
           "a header-only notify export is refused, not read as clean  "
           "<-- pinned defect")
    report = build_report(four, notify=all_rows, operators=[])
    check(kinds(report.findings) == ["NO_FAILURE_ALERT"] * 4 +
          ["MAIL_UNCHECKED"] and gate(report) == 1 and
          report.checked == ["failure alerts"],
          "a server with no operators flags every enabled job, and a "
          "header-only operators export is legitimate")
    report = build_report(four, notify=all_rows, operators=ops)
    check(kinds(report.findings) == ["MAIL_UNCHECKED"] and gate(report) == 3
          and "no --mail export was given" in report.findings[0].text,
          "notify without --mail warns that mail was not checked, so a clean "
          "run does not claim more than it read")
    lines = describe(report)
    check(lines[0] == "stepflow: 4 job(s), 0 step(s), 0 history row(s), 0 "
          "activity row(s), 4 notify row(s), 6 operator(s)",
          "the report opens with the notify and operator counts when they "
          "were read")
    report = build_report(four, notify=all_rows, operators=ops,
                          mail=Mail("Windows", 0, ""))
    check(kinds(report.findings) == ["MAIL_NEVER_SENDS"] and
          report.checked == ["failure alerts", "agent mail"] and
          gate(report) == 1,
          "dead mail is one finding, not one per job: every job names an "
          "operator, and the cause is filed once")
    report = build_report(four, mail=Mail("Windows", 1, "Ops mail", 1, 1))
    check(report.checked == ["agent mail"] and report.findings == [] and
          report.alert_counts is None and gate(report) == 0,
          "--mail alone checks the mail setting and nothing else")

    # ---- real files on disk, through main(). One temp directory, deleted
    # again at the end.
    tmp = tempfile.mkdtemp(prefix="stepflow-selftest-")
    # An unexpected exception must not leave the directory behind.
    atexit.register(shutil.rmtree, tmp, True)

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
    check(code == 2 and "line 2" in err and "step_id is a value of 3 characters"
          in err and "'one'" not in err,
          "a step_id that is not a number exits 2 and names the line")
    bad = tmpfile("badid.csv", "job_id,name,start_step_id\nnot-a-guid,x,1\n")
    code, _, err = run_cli(["--jobs", bad, "--steps", steps_csv])
    check(code == 2 and "a value of 10 characters is not a job id" in err and
          "not-a-guid" not in err and "NOCOUNT" not in err,
          "a job_id that is not a GUID exits 2")
    bad = tmpfile("nocol.csv", "job_id,step_id\n%s,x\n" % j_load)
    code, _, err = run_cli(["--jobs", bad, "--steps", steps_csv])
    check(code == 2 and "name, start_step_id" in err and
          "It has: job_id, step_id." in err and "-y 0" not in err,
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

    # ---- exports that look fine and say nothing
    bare = tmpfile("bare.csv", "job_id,name,start_step_id,eol\n")
    code, out, err = run_cli(["--jobs", bare, "--steps", steps_csv])
    check(code == 2 and "jobs export has no rows" in err and out == "",
          "a jobs export with a header and no rows exits 2, not a clean "
          "pass  <-- pinned defect")
    bare_act = tmpfile("bareact.csv", "job_id,start_execution_date,"
                       "stop_execution_date,exported_at,eol\n")
    code, out, err = run_cli(["--jobs", jobs_csv, "--activity", bare_act])
    check(code == 2 and "activity export has no rows" in err,
          "an activity export with no rows exits 2: it cannot show a "
          "running job  <-- pinned defect")
    bare_hist = tmpfile("barehist.csv", "instance_id,job_id,step_id,"
                        "run_status,run_date,run_time,run_duration,eol\n")
    code, out, _ = run_cli(["--jobs", jobs_csv, "--history", bare_hist])
    check(code == 3 and out.count("NO_BASELINE") == 3 and
          "no finished run" in out,
          "a history export with no rows warns once for every job")
    code, out, _ = run_cli(["--jobs", jobs_csv, "--history", history_csv])
    check(out.count("NO_BASELINE") == 2 and
          "watchdog, load: it has no finished run" in out,
          "a job with no run in a history export is NO_BASELINE, and a job "
          "that has runs is not  <-- pinned defect")

    # ---- paths, numbers and counts
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", ""])
    check(code == 2 and "error: " in err,
          "an empty --steps, an unset shell variable, exits 2 and is not "
          "skipped  <-- pinned defect")
    code, _, err = run_cli(["--jobs", jobs_csv, "--history", history_csv,
                            "--report", history_csv, "--apply"])
    with open(history_csv, encoding="utf-8") as handle:
        kept = handle.read()
    check(code == 64 and "names an input export" in err and
          kept.startswith("instance_id"),
          "--report naming an input export is refused, and the export is "
          "kept  <-- pinned defect")
    hist_rows = tmpfile("histrows.csv", "instance_id,job_id,step_id,"
                        "run_status,run_date,run_time,run_duration\n"
                        "(1 rows affected)\n")
    code, _, err = run_cli(["--jobs", jobs_csv, "--history", hist_rows])
    check(code == 2 and "SET NOCOUNT ON" in err and "-y 0" not in err,
          "a '(1 rows affected)' line in a history export names SET "
          "NOCOUNT ON, not -y 0  <-- pinned defect")
    spaced = tmpfile("spaced.csv", jobs_text + "   \r\n")
    code, out, _ = run_cli(["--jobs", spaced, "--steps", steps_csv])
    code8, out8, _ = run_cli(["--jobs", jobs_csv, "--steps", steps_csv])
    check(code == code8 and out == out8,
          "a trailing line of spaces is not a cut row  <-- pinned defect")
    for value in ("1_0", "\u0661"):
        bad = tmpfile("digit.csv", "job_id,step_id,step_name,"
                      "on_success_action,on_success_step_id,on_fail_action,"
                      "on_fail_step_id,command\n%s,%s,x,1,0,2,0,\n"
                      % (j_load, value))
        code, _, err = run_cli(["--jobs", jobs_csv, "--steps", bad])
        check(code == 2 and "not a whole number" in err,
              "%s is not read as a whole number  <-- pinned defect"
              % ascii(value))
    bad = tmpfile("overflow.csv", "instance_id,job_id,step_id,run_status,"
                  "run_date,run_time,run_duration\n1,%s,0,1,"
                  "99999999999999999999,0,1\n" % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--history", bad])
    check(code == 2 and "not a date and time" in err,
          "a run_date past a C long exits 2, not 1 with a traceback  "
          "<-- pinned defect")
    bad = tmpfile("secret.csv", "job_id,step_id,step_name,on_success_action,"
                  "on_success_step_id,on_fail_action,on_fail_step_id,command"
                  "\n%s,%sHunter2Secret,x,1,0,2,0,\n" % (j_load, "x" * 40))
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", bad])
    check(code == 2 and "line 2" in err and "Hunter2" not in err,
          "an error cuts the cell it quotes, so a shifted column is not "
          "copied into a log  <-- pinned defect")
    badlike = tmpfile("badlike.csv", "job_id,step_id,step_name,"
                      "on_success_action,on_success_step_id,on_fail_action,"
                      "on_fail_step_id,command\n%s,1,x,1,0,2,0,\"SELECT 1 "
                      "FROM msdb.dbo.sysjobs WHERE name LIKE N'x[^]'\"\n"
                      % j_load)
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", badlike])
    check(code == 2 and "not a valid character class" in err,
          "a LIKE pattern that is not a valid class exits 2, not 1 with a "
          "traceback  <-- pinned defect")
    code, _, err = run_cli(["--jobs", jobs_csv, "--steps", orphan])
    check("@delete_history" in err,
          "the orphan message names a deleted job's history rows too")
    hdr = tmpfile("hdrcase.csv", "Job_ID,Name,Start_Step_Id\n%s,hourly,1\n"
                  % j_hour)
    code, out, _ = run_cli(["--jobs", hdr, "--steps", clean_steps])
    check(code == 0, "column names are read ignoring case")
    for flag, value in (("--min-runs", "1"), ("--min-seconds", "0")):
        code, _, _ = run_cli(["--jobs", jobs_csv, "--steps", steps_csv,
                              flag, value])
        check(code == 1, "%s %s is accepted" % (flag, value))

    # ---- output that cannot be encoded, and a reader that quits
    arrow = tmpfile("arrow.csv", "job_id,name,start_step_id\n%s,load \u2192 "
                    "v2,1\n" % j_load)
    arrow_steps = tmpfile("arrowsteps.csv", "job_id,step_id,step_name,"
                          "on_success_action,on_success_step_id,"
                          "on_fail_action,on_fail_step_id,command\n"
                          "%s,1,a,1,0,2,0,\n%s,2,b,1,0,2,0,\n"
                          % (j_load, j_load))
    raw = io.BytesIO()
    narrow = io.TextIOWrapper(raw, encoding="ascii", newline="\n")
    with contextlib.redirect_stdout(narrow), \
            contextlib.redirect_stderr(io.StringIO()):
        code = main(["--jobs", arrow, "--steps", arrow_steps])
    narrow.flush()
    check(code == 1 and b"load ? v2: step 2 (b) never runs" in raw.getvalue(),
          "a job name outside the output code page prints with a ? and does "
          "not crash the report  <-- pinned defect")

    class Broken(io.StringIO):
        fd = None

        def write(self, text):
            raise BrokenPipeError("the reader quit")

        def fileno(self):
            if self.fd is None:
                raise io.UnsupportedOperation("fileno")
            return self.fd

    def quit_early(fd):
        stream = Broken()
        stream.fd = fd
        with contextlib.redirect_stdout(stream), \
                contextlib.redirect_stderr(io.StringIO()):
            return main(["--jobs", jobs_csv, "--steps", steps_csv])

    code = quit_early(None)
    check(code == 1, "a reader that quits early does not end the run in a "
          "traceback  <-- pinned defect")
    sink = os.path.join(tmp, "sink.bin")
    fd = os.open(sink, os.O_WRONLY | os.O_CREAT | getattr(os, "O_BINARY", 0))
    try:
        code = quit_early(fd)
        written = os.write(fd, b"x")
    finally:
        os.close(fd)
    check(code == 1 and written == 1 and os.path.getsize(sink) == 0,
          "and stdout is pointed at the null device, so exit cannot fail on "
          "the flush")

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
                        ("--min-seconds", "-1"), ("--factor", "nan"),
                        ("--factor", "inf"), ("--min-seconds", "nan"),
                        ("--min-seconds", "inf"),
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

    # ---- failure alerts, through main(). The first four fixtures are real
    # exports of the --sql notify, operators, mail and jobs SELECTs, taken
    # with sqlcmd -y 0 -f o:65001 from a SQL Server 2022 container on Linux.
    real_jobs = (
        "job_id,name,start_step_id,eol\n"
        "10B3E075-6770-42E0-9E5E-16510554BC66,\"sf disabled operator\",1,$\n"
        "07C397FE-AE28-41E7-907A-20B11AD943D4,\"sf failing\",1,$\n"
        "84A78982-1742-4FBC-8145-4E9977F2DF4A,\"sf orphan operator\",1,$\n"
        "255DF894-0012-4CD7-B944-52AFB317E2A6,\"sf netsend\",1,$\n"
        "A53BBC66-BE80-4DA3-A4CC-6FAA8ED06DDC,\"sf no operator\",1,$\n"
        "08476B3B-856B-4398-8455-72F1F4F837A1,\"sf healthy\",1,$\n"
        "96D3AC5F-9317-4DF3-BE0F-7DD375644AA9,\"sf pager ok\",1,$\n"
        "AC0C4D71-3C24-47CA-AB1A-A2A90C3B905B,\"sf success only\",1,$\n"
        "FBF84BDF-9AB2-4C0F-AF55-B08877370A6B,\"sf disabled job\",1,$\n"
        "A58527A0-9E00-4FAF-B37E-B1AB724E3B1F,\"sf blank address\",1,$\n"
        "44A04323-B954-4430-897C-B2AF71AB8636,\"sf pager never\",1,$\n"
        "FD3AA55C-01E5-46A2-B055-EEB81D191D58,\"sf level no operator\",1,$\n"
    )
    real_notify = (
        "job_id,enabled,email_level,email_operator_id,page_level,page_operator_id,netsend_level,netsend_operator_id,eol\n"
        "10B3E075-6770-42E0-9E5E-16510554BC66,1,3,2,0,0,0,0,$\n"
        "07C397FE-AE28-41E7-907A-20B11AD943D4,1,2,1,0,0,0,0,$\n"
        "84A78982-1742-4FBC-8145-4E9977F2DF4A,1,2,99,0,0,0,0,$\n"
        "255DF894-0012-4CD7-B944-52AFB317E2A6,1,0,0,0,0,2,1,$\n"
        "A53BBC66-BE80-4DA3-A4CC-6FAA8ED06DDC,1,0,0,0,0,0,0,$\n"
        "08476B3B-856B-4398-8455-72F1F4F837A1,1,2,1,0,0,0,0,$\n"
        "96D3AC5F-9317-4DF3-BE0F-7DD375644AA9,1,0,0,2,5,0,0,$\n"
        "AC0C4D71-3C24-47CA-AB1A-A2A90C3B905B,1,1,1,0,0,0,0,$\n"
        "FBF84BDF-9AB2-4C0F-AF55-B08877370A6B,0,0,0,0,0,0,0,$\n"
        "A58527A0-9E00-4FAF-B37E-B1AB724E3B1F,1,2,3,0,0,0,0,$\n"
        "44A04323-B954-4430-897C-B2AF71AB8636,1,0,0,2,4,0,0,$\n"
        "FD3AA55C-01E5-46A2-B055-EEB81D191D58,1,0,0,0,0,0,0,$\n"
    )
    real_operators = (
        "id,name,enabled,email_address,pager_address,pager_days,eol\n"
        "3,\"sf blank \"\"quoted\"\", op\",1,\"\",\"\",0,$\n"
        "2,\"sf off duty\",0,\"off@example.invalid\",\"\",0,$\n"
        "1,\"sf ops\",1,\"ops@example.invalid\",\"\",0,$\n"
        "4,\"sf pager never\",1,\"\",\"pg1@example.invalid\",0,$\n"
        "5,\"sf pager ok\",1,\"\",\"pg2@example.invalid\",127,$\n"
    )
    real_mail = (
        "platform,use_databasemail,databasemail_profile,profile_exists,"
        "database_mail_xps,eol\n"
        "Linux,1,\"\",0,0,$\n"
    )
    ra_jobs = tmpfile("ra_jobs.csv", real_jobs)
    ra_notify = tmpfile("ra_notify.csv", real_notify)
    ra_ops = tmpfile("ra_operators.csv", real_operators)
    ra_mail = tmpfile("ra_mail.csv", real_mail)
    code, out, err = run_cli(["--jobs", ra_jobs, "--notify", ra_notify,
                              "--operators", ra_ops, "--mail", ra_mail])
    check(code == 1 and err == "" and out.count("FAIL  NO_FAILURE_ALERT") == 8
          and "stepflow: 12 job(s), 0 step(s), 0 history row(s), 0 activity "
          "row(s), 12 notify row(s), 5 operator(s)" in out and
          "checked: failure alerts, agent mail" in out,
          "the real exports: eight jobs tell nobody, and the run exits 1")
    for name, why in (
            ("sf blank address", "which has no e-mail address"),
            ("sf disabled operator", "'sf off duty', which is disabled"),
            ("sf netsend", "not supported"),
            ("sf no operator", "no notification channel is set"),
            ("sf orphan operator", "operator id 99"),
            ("sf pager never", "pager_days is 0"),
            ("sf success only", "'on success only'")):
        check(any(line.startswith("FAIL  NO_FAILURE_ALERT       %s: " % name)
                  and why in line for line in out.splitlines()),
              "the real export flags '%s': %s" % (name, why))
    check('operator \'sf blank "quoted", op\'' in out,
          "an operator name with a quote and a comma survives the CSV")
    check("@" not in out and "example.invalid" not in out,
          "the report prints no operator e-mail or pager address")
    check(not any(("%s: " % name) in out for name in (
        "sf healthy", "sf pager ok", "sf failing", "sf disabled job")),
          "a working e-mail, a working pager, and a disabled job are clean")
    check("WARN  MAIL_UNCHECKED         (SQL Server Agent): this export came "
          "from SQL Server on Linux" in out and
          "MAIL_NEVER_SENDS" not in out,
          "the Linux mail export is MAIL_UNCHECKED, never a false "
          "MAIL_NEVER_SENDS")
    check("sf level no operator: no operator is told when this job fails: "
          "no notification channel is set." in out,
          "sp_update_job with a level and no operator stored level 0, and "
          "the job reads as no channel set")

    win_mail = real_mail.replace("Linux,1", "Windows,0")
    legacy = tmpfile("ra_legacy.csv",
                     win_mail.replace('"",0,0,$', '"Old Profile",0,0,$'))
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", legacy])
    check(code == 1 and "FAIL  MAIL_NEVER_SENDS       (SQL Server Agent): "
          "Agent mail is not enabled: use_databasemail is 0, and the profile "
          "'Old Profile' is set." in out and "checked: agent mail" in out,
          "a hand-written Windows mail row with a profile and use_databasemail "
          "0 exits 1, and the finding names the profile  <-- pinned defect")
    nullish = tmpfile("ra_null.csv", "platform,use_databasemail,"
                      "databasemail_profile\nWindows,NULL,\n")
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", nullish])
    check(code == 1 and "use_databasemail is 0" in out,
          "a NULL use_databasemail reads as not enabled")
    mail_head = real_mail.splitlines()[0] + "\n"
    good = tmpfile("ra_good.csv", mail_head + 'Windows,1,"Ops mail",1,1,$\n')
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", good])
    check(code == 0 and "VERDICT: no finding fails the gate, 0 warning(s)."
          in out, "database mail with a profile that exists and the XPs on "
          "exits 0")
    typo = tmpfile("ra_typo.csv", mail_head + 'Windows,1,"no such",0,1,$\n')
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", typo])
    check(code == 1 and "'no such' is not in msdb.dbo.sysmail_profile" in out,
          "a mistyped profile name exits 1  <-- pinned defect")
    xps_off = tmpfile("ra_xps.csv",
                      mail_head + 'Windows,1,"Ops mail",1,0,$\n')
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", xps_off])
    check(code == 1 and "'Database Mail XPs' option is 0" in out,
          "database mail XPs off exits 1  <-- pinned defect")
    old_shape = tmpfile("ra_old.csv", "platform,use_databasemail,"
                        "databasemail_profile\nWindows,1,\"Ops mail\"\n")
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", old_shape])
    check(code == 3 and "WARN  MAIL_UNCHECKED" in out and
          "1 warning(s)" in out,
          "a mail export without the two check columns warns, and does not "
          "claim a clean result")
    linux_mail = tmpfile("ra_linux.csv", "platform,use_databasemail,"
                         "databasemail_profile\nLinux,0,\n")
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", linux_mail])
    check(code == 3 and "could not be made, so this is not a clean run."
          in out and "no finding fails the gate, 1 warning(s), but" in out,
          "a Linux mail export exits 3, not 0: the setting was not read  "
          "<-- pinned defect")
    hung_jobs = tmpfile("hung_jobs.csv", "job_id,name,start_step_id\n"
                        "255df894-0012-4cd7-b944-52afb317e2a6,sf nightly,1\n")
    hung_hist = tmpfile("hung_hist.csv", "instance_id,job_id,step_id,"
                        "run_status,run_date,run_time,run_duration\n"
                        "1,255df894-0012-4cd7-b944-52afb317e2a6,0,1,"
                        "20261101,30000,100\n"
                        "2,255df894-0012-4cd7-b944-52afb317e2a6,0,1,"
                        "20261102,30000,100\n"
                        "3,255df894-0012-4cd7-b944-52afb317e2a6,0,3,"
                        "20261103,30000,400000\n")
    code, out, _ = run_cli(["--jobs", hung_jobs, "--history", hung_hist])
    check(code == 3 and "NO_BASELINE" in out and "FAR_PAST_NORMAL" not in out,
          "a run of a job with too few runs to time exits 3, not 0  "
          "<-- pinned defect")
    report = build_report([job()], [step(1, fail=QUIT_SUCCESS), step(2)],
                          activity=[act(90)])
    check(gate(report) == 1 and report.untold,
          "a failing finding still exits 1 when a check was also not made")
    bad_flag = tmpfile("ra_flag.csv",
                       mail_head + 'Windows,1,"Ops mail",2,1,$\n')
    code, _, err = run_cli(["--jobs", ra_jobs, "--mail", bad_flag])
    check(code == 2 and "profile_exists is 2, and only 0, 1 are defined"
          in err, "a profile_exists of 2 exits 2")
    blank_checks = tmpfile("ra_blank.csv",
                           mail_head + 'Windows,1,"Ops mail",NULL,,$\n')
    code, out, _ = run_cli(["--jobs", ra_jobs, "--mail", blank_checks])
    check(code == 3 and "WARN  MAIL_UNCHECKED" in out,
          "NULL or empty check columns read as unknown")
    steps_row = ('11111111-1111-1111-1111-111111111111,1,"load",1,0,2,0,'
                 '"sqlcmd -S srv -U sa -P Hunter2pass! -Q ""select 1""",$\n')
    headerless = tmpfile("ra_nohdr.csv", steps_row)
    code, out, err = run_cli(["--jobs", ra_jobs, "--steps", headerless])
    check(code == 2 and "columns not in the export" in err and
          "hunter2pass" not in err.lower() and "sqlcmd" not in err.lower()
          and "<cell>" in err,
          "a steps export with no header row does not echo its first "
          "command to stderr  <-- pinned defect")
    steps_head = ("job_id,step_id,step_name,on_success_action,"
                  "on_success_step_id,on_fail_action,on_fail_step_id,"
                  "command,eol\n")
    guid = "11111111-1111-1111-1111-111111111111"
    for label, text, column in (
            ("an integer cell", guid + ',1,load,"sqlcmd -U sa -P Hunter2pass'
             ' -Q x",1,0,2,0,$\n', "on_success_action"),
            ("a job id cell", '"sqlcmd -S srv -U sa -P Hunter2",1,load,1,0,'
             '2,0,x,$\n', "not a job id")):
        shifted = tmpfile("ra_shift.csv", steps_head + text)
        code, out, err = run_cli(["--jobs", ra_jobs, "--steps", shifted])
        check(code == 2 and "line 2" in err and column in err and
              "hunter2" not in err.lower() and "sqlcmd" not in err.lower()
              and "a value of" in err,
              "a command in %s is not copied into the error  "
              "<-- pinned defect" % label)
    ops_head = "id,name,enabled,email_address,pager_address,pager_days,eol\n"
    shifted = tmpfile("ra_opshift.csv", ops_head + 'ops@example.invalid,'
                      '"sf ops",1,"",0,0,$\n')
    code, out, err = run_cli(["--jobs", ra_jobs, "--notify", ra_notify,
                              "--operators", shifted])
    check(code == 2 and "line 2" in err and "ops@example" not in err,
          "an e-mail address in the id cell is not copied into the error  "
          "<-- pinned defect")
    check(show_cell("12") == "'12'" and show_cell("") == "''" and
          show_cell("x" * 25) == "a value of 25 characters" and
          show_cell("2026-11-10 01:02:03") == "'2026-11-10 01:02:03'" and
          show_cell("Hunter2") == "a value of 7 characters" and
          show_cell("-P Hunter2") == "a value of 10 characters" and
          show_cell("a@b") == "a value of 3 characters",
          "show_cell() quotes a number or a time and only counts the rest  "
          "<-- pinned defect")
    for label, text in (
            ("a fragment of a command", guid + ',1,"load",-P Hunter2,0,2,0,'
             '"cmd",$\n'),
            ("a one-word password", guid + ',1,"load",Hunter2,0,2,0,"cmd",'
             '$\n')):
        shifted = tmpfile("ra_frag.csv", steps_head + text)
        code, out, err = run_cli(["--jobs", ra_jobs, "--steps", shifted])
        check(code == 2 and "line 2" in err and "hunter2" not in err.lower()
              and "a value of" in err,
              "%s in a number cell is not copied into the error  "
              "<-- pinned defect" % label)
    sel = tmpfile("ra_selhdr.csv", guid + ',1,"load",1,0,2,0,"select '
                  'secretword from t",$\n')
    code, out, err = run_cli(["--jobs", ra_jobs, "--steps", sel])
    check(code == 2 and "secretword" not in err and "<cell>" in err,
          "a command of lowercase words in the header row is not copied "
          "into the error  <-- pinned defect")
    rag_head = ("job_id,step_id,step_name,on_success_action,"
                "on_success_step_id,on_fail_action,on_fail_step_id,command\n")
    for label, text, cells in (
            ("an unquoted comma in a command", guid + ',1,w,1,0,2,0,exec '
             'sp_stop_job @x=1, @job_name = N\'Gone\'\n', "9 cells"),
            ("a row one cell short", guid + ',1,w,1,0,2,0\n', "7 cells")):
        ragged = tmpfile("ra_ragged.csv", rag_head + text)
        code, out, err = run_cli(["--jobs", ra_jobs, "--steps", ragged])
        check(code == 2 and "line 2" in err and cells in err and
              "VERDICT" not in out,
              "%s without an eol column exits 2, not 0  "
              "<-- pinned defect" % label)
    wide = tmpfile("ra_wide.csv", ",".join("c%d" % n for n in range(20)) +
                   "\n")
    code, out, err = run_cli(["--jobs", ra_jobs, "--steps", wide])
    check(code == 2 and err.count("<cell>") == 12 and ", ..." in err,
          "a header of many cells names twelve and then stops")
    code, out, _ = run_cli(["--jobs", ra_jobs, "--notify", ra_notify,
                            "--operators", ra_ops, "--mail", good])
    check(code == 1 and "checked: failure alerts, agent mail" in out and
          "MAIL_UNCHECKED" not in out and out.count("NO_FAILURE_ALERT") == 8,
          "with a good mail row the per-job findings stand alone")

    def alert_error(label, expect, notify_text=None, ops_text=None,
                    mail_text=None, code_wanted=2):
        argv = ["--jobs", ra_jobs]
        for flag, text in (("--notify", notify_text),
                           ("--operators", ops_text), ("--mail", mail_text)):
            if text is not None:
                argv += [flag, tmpfile("ae%s.csv" % flag[2:], text)]
        code, _, err = run_cli(argv)
        check(code == code_wanted and expect in err, label)

    head_n = real_notify.splitlines()[0]
    first_n = real_notify.splitlines()[1]
    alert_error("a notify level of 4 exits 2 and names the column",
                "email_level is 4, and only 0, 1, 2, 3 are defined",
                head_n + "\n" + first_n.replace(",1,3,2,", ",1,4,2,") + "\n",
                real_operators)
    alert_error("a negative operator id exits 2",
                "email_operator_id is -1, and an id is not negative",
                head_n + "\n" + first_n.replace(",3,2,", ",3,-1,") + "\n",
                real_operators)
    alert_error("an enabled flag of 2 exits 2",
                "enabled is 2, and only 0, 1 are defined",
                head_n + "\n" + first_n.replace(",1,3,", ",2,3,") + "\n",
                real_operators)
    alert_error("a job missing from the notify export exits 2 and names it",
                "the notify export has no row for job id",
                head_n + "\n" + first_n + "\n", real_operators)
    alert_error("a header-only notify export exits 2, not a clean pass",
                "the notify export has no rows", head_n + "\n",
                real_operators)
    alert_error("a pager_days of 128 exits 2",
                "pager_days is 128, and only 0, 1,",
                real_notify, real_operators.replace(",0,$", ",128,$", 1))
    alert_error("an operators export without a column exits 2 and names it",
                "columns not in the export: pager_days", real_notify,
                real_operators.replace("pager_days,", ""))
    alert_error("a mail export with two rows exits 2",
                "has 2 rows", None, None,
                real_mail + real_mail.splitlines()[1] + "\n")
    alert_error("a header-only mail export exits 2",
                "has 0 rows", None, None, real_mail.splitlines()[0] + "\n")
    alert_error("use_databasemail of 2 exits 2",
                "use_databasemail is 2, and only 0, 1 are defined", None,
                None, real_mail.replace("Linux,1", "Windows,2"))
    alert_error("--notify without --operators is a usage error",
                "--notify and --operators go together", real_notify,
                None, None, 64)
    alert_error("--operators without --notify is a usage error",
                "--notify and --operators go together", None,
                real_operators, None, 64)
    code, _, err = run_cli(["--jobs", ra_jobs, "--notif", ra_notify,
                            "--operators", ra_ops])
    check(code == 64, "a prefix --notif of --notify is refused  "
          "<-- pinned defect")
    code, _, err = run_cli(["--jobs", ra_jobs, "--mail", good, "--report",
                            good, "--apply"])
    check(code == 64 and "--report names an input export" in err,
          "--report cannot name the --mail export")
    code, _, err = run_cli(["--jobs", ra_jobs, "--notify", ra_notify,
                            "--operators", ra_ops, "--report", ra_ops,
                            "--apply"])
    check(code == 64 and "--report names an input export" in err,
          "--report cannot name the --operators export")
    before = set(os.listdir(tmp))
    code, out, _ = run_cli(["--jobs", ra_jobs, "--notify", ra_notify,
                            "--operators", ra_ops, "--mail", ra_mail,
                            "--report", os.path.join(tmp, "alerts.txt")])
    check(code == 1 and set(os.listdir(tmp)) == before and
          "nothing was written" in out,
          "the alert checks write nothing without --apply")
    for name in ("notify", "operators", "mail"):
        code, out, _ = run_cli(["--sql", name])
        check(code == 0 and out == SQL[name] and "$')" in out and
              "SET NOCOUNT ON;" in out,
              "--sql %s prints its SELECT, with the eol column and "
              "NOCOUNT" % name)
    check("password" not in "".join(SQL.values()).lower() and
          "host_login" not in "".join(SQL.values()).lower(),
          "no SELECT reads a password column: sp_get_sqlagent_properties "
          "returns one, and the mail SELECT reads two registry values "
          "instead")

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
    with open(os.path.join(here, "stepflow.py"), "rb") as handle:
        check(max(bytearray(handle.read())) < 128,
              "the source file is pure ASCII  <-- pinned defect")

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

_KNOWN_HEADERS = frozenset(
    [c for columns in COLUMNS.values() for c in columns] + ["eol", "line"])

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
        # A file with no header row puts its first data row here, and a steps
        # row holds a command. Name only cells that are a known column name.
        names = [h if h in _KNOWN_HEADERS else "<cell>"
                 for h in header[:12]]
        if len(header) > 12:
            names.append("...")
        raise ValueError("%s: columns not in the export: %s. It has: %s.%s"
                         % (path, ", ".join(absent),
                            ", ".join(names) or "(no columns at all)",
                            hint))
    rows = []
    end = reader.line_num
    for cells in reader:
        # A record can span lines, so name the line it starts on.
        line, end = end + 1, reader.line_num
        if not any(cell.strip() for cell in cells):
            continue
        row = dict((h, "") for h in header)
        row.update(zip(header, cells))
        row["_line"] = line
        if "rows affected" in cells[0]:
            raise ValueError("%s line %d: the row is a row count. Put SET "
                             "NOCOUNT ON at the top of the query."
                             % (path, line))
        if "eol" in header and row["eol"].strip() != "$":
            raise ValueError("%s line %d: the row is cut short. Run sqlcmd "
                             "with -y 0, or it cuts every long value at 256 "
                             "characters." % (path, line))
        # zip() above drops an extra cell and pads a missing one, so a
        # command cut at an unquoted comma would read as a whole row.
        if len(cells) != len(header):
            raise ValueError("%s line %d: the row has %d cells and the "
                             "header has %d. A client that splits on a "
                             "separator cannot write a correct CSV."
                             % (path, line, len(cells), len(header)))
        rows.append(row)
    return rows


_INT = re.compile(r"\A[+-]?[0-9]+\Z")


def _cell_int(row, column, path):
    value = row.get(column, "").strip()
    # int() alone also reads 1_0 and digits from other scripts.
    if not _INT.match(value):
        raise ValueError("%s line %d: %s is %s, not a whole number"
                         % (path, row["_line"], column, show_cell(value)))
    return int(value)


def _cell_job(row, path):
    value = row["job_id"].strip()
    if not _GUID.match(value):
        raise ValueError("%s line %d: a value of %d characters is not a job id"
                         % (path, row["_line"], len(value)))
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
        raise ValueError("%s line %d: %s is %s, not YYYY-MM-DD HH:MM:SS"
                         % (path, row["_line"], column, show_cell(value)))


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
                                              r[column].strip()[:32]))
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


def _cell_choice(row, column, path, allowed):
    value = _cell_int(row, column, path)
    if value not in allowed:
        raise ValueError("%s line %d: %s is %d, and only %s are defined"
                         % (path, row["_line"], column, value,
                            ", ".join(str(a) for a in allowed)))
    return value


def _cell_id(row, column, path):
    value = _cell_int(row, column, path)
    if value < 0:
        raise ValueError("%s line %d: %s is %d, and an id is not negative"
                         % (path, row["_line"], column, value))
    return value


def load_notify(path):
    rows = []
    for r in read_table(path, COLUMNS["notify"]):
        channels = [(_cell_choice(r, "%s_level" % c, path, (0, 1, 2, 3)),
                     _cell_id(r, "%s_operator_id" % c, path))
                    for c in ("email", "page", "netsend")]
        rows.append(Notify(_cell_job(r, path),
                           _cell_choice(r, "enabled", path, (0, 1)),
                           *channels))
    return rows


def load_operators(path):
    return [Operator(_cell_id(r, "id", path), r["name"],
                     _cell_choice(r, "enabled", path, (0, 1)),
                     r["email_address"], r["pager_address"],
                     _cell_choice(r, "pager_days", path, range(128)))
            for r in read_table(path, COLUMNS["operators"])]


def load_mail(path):
    rows = read_table(path, COLUMNS["mail"])
    if len(rows) != 1:
        raise ValueError("%s has %d rows. The Agent mail export has exactly "
                         "one. Check that the query ran on the right "
                         "server." % (path, len(rows)))
    r = rows[0]
    # A value the registry does not hold reads as NULL or empty: not enabled.
    if r["use_databasemail"].strip() in ("", "NULL"):
        r["use_databasemail"] = "0"
    checks = []
    for column in ("profile_exists", "database_mail_xps"):
        # A column the export lacks, or leaves empty, is not known.
        if r.get(column, "").strip() in ("", "NULL"):
            checks.append(None)
        else:
            checks.append(_cell_choice(r, column, path, (0, 1)))
    return Mail(r["platform"], _cell_choice(r, "use_databasemail", path,
                                            (0, 1)),
                r["databasemail_profile"], *checks)


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
                    "watchdogs bound to a job name, runs far past their "
                    "normal duration, and jobs that tell nobody when they "
                    "fail, from exported msdb rows.",
        epilog="Nothing is written without --apply.",
        allow_abbrev=False,
    )
    ap.add_argument("--jobs", help="the msdb.dbo.sysjobs export (--sql jobs)")
    ap.add_argument("--steps", help="the sysjobsteps export (--sql steps)")
    ap.add_argument("--history",
                    help="the sysjobhistory export (--sql history)")
    ap.add_argument("--activity",
                    help="the sysjobactivity export (--sql activity)")
    ap.add_argument("--notify",
                    help="the sysjobs notification export (--sql notify). "
                         "Needs --operators.")
    ap.add_argument("--operators",
                    help="the sysoperators export (--sql operators). Needs "
                         "--notify.")
    ap.add_argument("--mail",
                    help="the Agent mail setting export (--sql mail)")
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


def _emit(lines):
    """Print lines. A reader that quits early (| head) ends the output, not
    the run: exit 1 means findings, and a traceback must not claim that."""
    try:
        for line in lines:
            print(line)
        sys.stdout.flush()
    except BrokenPipeError:
        # Point stdout at the null device, or the flush at exit fails again.
        try:
            null = os.open(os.devnull, os.O_WRONLY)
            try:
                os.dup2(null, sys.stdout.fileno())
            finally:
                os.close(null)
        except (OSError, ValueError):
            pass


def main(argv=None):
    # A job or step name outside the console code page must not crash the
    # report: that traceback would exit 1, which reads as findings.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(errors="replace")
    args = _parse(sys.argv[1:] if argv is None else argv)

    if args.self_test:
        return self_test()
    if args.sql:
        sys.stdout.write(SQL[args.sql])
        return 0
    # "is None", not truthiness: --steps "" (an unset shell variable) is a
    # path that cannot be opened, and exits 2, not a check that is skipped.
    paths = (args.jobs, args.steps, args.history, args.activity, args.notify,
             args.operators, args.mail)
    if args.jobs is None:
        return _usage("--jobs is required. Print its SELECT with --sql jobs.")
    if paths[1:] == (None,) * 6:
        return _usage("give --steps, --history or --activity as well, or "
                      "--notify with --operators, or --mail. --jobs alone "
                      "checks nothing.")
    if (args.notify is None) != (args.operators is None):
        return _usage("--notify and --operators go together: an operator id "
                      "means nothing without the operators.")
    # Written as a range test so that nan, which fails every comparison, is
    # refused too. inf would turn the timing checks off.
    if not 1 < args.factor < float("inf"):
        return _usage("--factor must be more than 1 and finite, or a normal "
                      "run is flagged or no run is.")
    if args.min_runs < 1:
        return _usage("--min-runs must be 1 or more.")
    if not 0 <= args.min_seconds < float("inf"):
        return _usage("--min-seconds must be 0 or more and finite.")
    if args.apply and not args.report:
        return _usage("--apply needs --report.")
    inputs = [os.path.normcase(os.path.abspath(p)) for p in paths if p]
    if args.report and os.path.normcase(os.path.abspath(args.report)) in \
            inputs:
        return _usage("--report names an input export, and --apply would "
                      "overwrite it.")
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
        steps = None if args.steps is None else load_steps(args.steps)
        history = None if args.history is None else load_history(args.history)
        activity = None if args.activity is None else \
            load_activity(args.activity)
        notify = None if args.notify is None else load_notify(args.notify)
        operators = None if args.operators is None else \
            load_operators(args.operators)
        mail = None if args.mail is None else load_mail(args.mail)
        report = build_report(jobs, steps, history, activity, args.factor,
                              args.min_runs, args.min_seconds, before,
                              notify, operators, mail)
    except (ValueError, OSError, csv.Error) as exc:
        # csv.Error too: a field over csv's limit, or a NUL in the file, is
        # an export that could not be read, not a finding.
        print("error: %s" % exc, file=sys.stderr)
        return 2

    lines = describe(report)
    _emit(lines)

    if args.report:
        if not args.apply:
            _emit(["", "--report without --apply: nothing was written."])
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
            _emit(["", "wrote %s" % args.report])
    return gate(report)


if __name__ == "__main__":
    sys.exit(main())
