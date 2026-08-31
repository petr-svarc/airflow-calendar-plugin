import re
import hashlib
from datetime import datetime, timedelta, timezone as dt_timezone

from croniter import croniter
from sqlalchemy import desc

import pendulum

from airflow.models import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone

from airflow_calendar.dag_colors import get_dag_color, load_dag_colors

IGNORED_DAGS = ["airflow_monitoring"]
RUNS_COUNT = 5000


def parse_timedelta_schedule(schedule):
    """
    Return a timedelta if schedule is a timedelta object or a timedelta string
    like '1 day, 6:00:00' or '30:00:00'. Returns None otherwise.
    """
    if isinstance(schedule, timedelta):
        return schedule

    if not isinstance(schedule, str):
        return None

    cleaned_schedule = schedule.strip()

    match_with_days = re.match(
        r'^(\d+)\s+days?,\s*(\d+):(\d+):(\d+)$', cleaned_schedule)
    if match_with_days:
        days = int(match_with_days.group(1))
        hours = int(match_with_days.group(2))
        minutes = int(match_with_days.group(3))
        seconds = int(match_with_days.group(4))
        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    match_time_only = re.match(r'^(\d+):(\d+):(\d+)$', cleaned_schedule)
    if match_time_only:
        hours = int(match_time_only.group(1))
        minutes = int(match_time_only.group(2))
        seconds = int(match_time_only.group(3))
        parsed_timedelta = timedelta(
            hours=hours, minutes=minutes, seconds=seconds)
        if parsed_timedelta.total_seconds() > 0:
            return parsed_timedelta

    return None


def get_color_from_tag(tag_name):
    hash_object = hashlib.md5(tag_name.encode())
    return "#" + hash_object.hexdigest()[:6]


def get_border_color(status):
    if status == 'success':
        return "#28a745"
    if status == 'failed':
        return "#dc3545"
    if status == 'running':
        return "#017cee"
    return "#808080"


def get_avg_execution_time(recent_success_runs):
    default_avg_seconds = 300
    if recent_success_runs:
        durations = [(run.end_date - run.start_date).total_seconds()
                     for run in recent_success_runs if run.start_date]
        if durations:
            return max((sum(durations) / len(durations)), default_avg_seconds)
    return default_avg_seconds


def get_schedule_info(dag):
    schedule = getattr(dag, 'schedule_interval', None)
    if schedule is None:
        schedule = getattr(dag, 'timetable_summary', None)
    if schedule is None and hasattr(dag, 'schedule'):
        schedule = dag.schedule
    return schedule


def get_dag_task_count(session, dagbag, dag):
    ser_dag = SerializedDagModel.get(dag.dag_id, session=session)
    if ser_dag:
        return len(ser_dag.dag.tasks)
    loaded_dag = dagbag.get_dag(dag.dag_id)
    return len(loaded_dag.tasks) if loaded_dag else 0


def _format_duration(avg_seconds):
    return f"{int(avg_seconds / 60)}m {int(avg_seconds % 60)}s"


def _build_history_data(recent_runs, date_attr, limit=5):
    history = []
    for run in reversed(recent_runs[:limit]):
        run_date = getattr(run, date_attr, None)
        if run_date is None:
            continue
        history.append({
            "state": run.state,
            "date": run_date.strftime('%d/%m/%Y %H:%M'),
        })
    return history


# def _make_calendar_event(dag, event_time, avg_seconds, bg_color, border_color,
#                          status, schedule, task_count, history_data):
#     return {
#         "title": dag.dag_id,
#         "start": event_time.isoformat() + 'Z',
#         "end": (event_time + timedelta(seconds=avg_seconds)).isoformat() + 'Z',
#         "backgroundColor": bg_color,
#         "borderColor": border_color,
#         "borderWidth": "3px",
#         "extendedProps": {
#             "status": status,
#             "cron": schedule if isinstance(schedule, str) else str(schedule),
#             "duration": _format_duration(avg_seconds),
#             "dag_id": dag.dag_id,
#             "task_count": int(task_count),
#             "history": history_data,
#         },
#     }


def _add_cron_events(events, dag, schedule, cron_start, cron_end, run_history,
                     avg_seconds, bg_color, task_count, history_data):
    cron = croniter(schedule, cron_start)
    for _ in range(RUNS_COUNT):
        event_time = cron.get_next(datetime)
        if event_time > cron_end:
            break

        current_iso = event_time.replace(microsecond=0).isoformat()
        status = run_history.get(current_iso, "no_run")
        events.append(_make_calendar_event(
            dag, event_time, avg_seconds, bg_color,
            get_border_color(status), status, schedule,
            task_count, history_data,
        ))


def _timedelta_anchor_time(dag, recent_runs, date_attr):
    """Pick a reference instant to project timedelta slots (like cron uses cron_start)."""
    now_naive = datetime.now(dt_timezone.utc).replace(tzinfo=None)

    for run in recent_runs:
        run_date = getattr(run, date_attr, None)
        if run_date is None:
            continue
        run_time = run_date.replace(tzinfo=None)
        if run_time <= now_naive:
            return run_time

    for run in reversed(recent_runs):
        run_date = getattr(run, date_attr, None)
        if run_date is None:
            continue
        # Airflow 2 may only have the next scheduled run before the first execution.
        return run_date.replace(tzinfo=None)

    next_dagrun = getattr(dag, 'next_dagrun', None)
    if next_dagrun is not None:
        return next_dagrun.replace(tzinfo=None)

    return None


def _add_timedelta_events(events, dag, schedule, schedule_delta, recent_runs,
                          date_attr, cron_start, cron_end, run_history,
                          avg_seconds, bg_color, task_count, history_data):

    anchor_time = _timedelta_anchor_time(dag, recent_runs, date_attr)
    if anchor_time is None:
        return
    current_event_time = anchor_time
    while current_event_time >= cron_start:
        current_event_time -= schedule_delta
    current_event_time += schedule_delta

    rendered_count = 0
    while current_event_time <= cron_end and rendered_count < RUNS_COUNT:
        current_iso = current_event_time.replace(microsecond=0).isoformat()
        status = run_history.get(current_iso, "no_run")
        events.append(_make_calendar_event(
            dag, current_event_time, avg_seconds, bg_color,
            get_border_color(status), status, schedule,
            task_count, history_data,
        ))
        current_event_time += schedule_delta
        rendered_count += 1


##### START
# def build_calendar_events(session, dags, dagbag, date_col, date_attr):
#     events = []
#     now = timezone.utcnow()
#     start_search = now - timedelta(days=7)
#     end_search = now + timedelta(days=7)

#     cron_start = start_search.replace(tzinfo=None)
#     cron_end = end_search.replace(tzinfo=None)
#     dag_colors = load_dag_colors()

#     for dag in dags:
#         if dag.dag_id in IGNORED_DAGS:
#             continue

#         task_count = get_dag_task_count(session, dagbag, dag)
#         schedule = get_schedule_info(dag)

#         dag_runs = session.query(DagRun).filter(
#             DagRun.dag_id == dag.dag_id,
#             date_col >= start_search,
#             date_col <= end_search,
#         ).all()

#         run_history = {
#             getattr(run, date_attr).replace(tzinfo=None, microsecond=0).isoformat(): run.state
#             for run in dag_runs
#         }

#         recent_runs = session.query(DagRun).filter(
#             DagRun.dag_id == dag.dag_id,
#             date_col.isnot(None),
#         ).order_by(desc(date_col)).limit(15).all()

#         recent_success_runs = [
#             run for run in recent_runs
#             if run.state == 'success' and run.end_date
#         ][:5]
#         avg_seconds = get_avg_execution_time(recent_success_runs)
#         history_data = _build_history_data(recent_runs, date_attr)
#         bg_color = get_dag_color(dag.dag_id, dag_colors)

#         if schedule and isinstance(schedule, str) and croniter.is_valid(schedule):
#             try:
#                 _add_cron_events(
#                     events, dag, schedule, cron_start, cron_end,
#                     run_history, avg_seconds, bg_color,
#                     task_count, history_data,
#                 )
#             except Exception:
#                 continue
#         else:
#             schedule_delta = parse_timedelta_schedule(schedule)
#             if schedule_delta:
#                 try:
#                     _add_timedelta_events(
#                         events, dag, schedule, schedule_delta, recent_runs,
#                         date_attr, cron_start, cron_end, run_history,
#                         avg_seconds, bg_color, task_count, history_data,
#                     )
#                 except Exception:
#                     continue

#     return events



########################################################################################################################

def _build_dagrun_detail(dag, dag_date, scheduled_run, executed_run, dag_color):
    return {
        "dag_id": dag.dag_id,
        "dag_date": dag_date.in_tz('UTC'),
        "display_name": dag.dag_display_name,
        "dag_color": dag_color,
        "task_count": len(dag.task_ids),
        "run_at": scheduled_run.run_after if scheduled_run else pendulum.instance(executed_run.start_date) if executed_run else None,
        "dagrun_type": 'scheduled & executed' if scheduled_run and executed_run else 'scheduled' if scheduled_run else 'executed' if executed_run else None,
        "schedule": dag.schedule_interval,
        "timezone": dag.timezone,
        "logical_date": scheduled_run.logical_date if scheduled_run else None,
        "schedule_data_interval": pendulum.interval(pendulum.instance(scheduled_run.data_interval.start),
                                                    pendulum.instance(scheduled_run.data_interval.end)
                                                   ) if scheduled_run else None,
        "run_state": executed_run.state if executed_run else 'no_run',
        "execution_date": pendulum.instance(executed_run.execution_date) if executed_run else None,
        "start_date": pendulum.instance(executed_run.start_date) if executed_run else None,
        "end_date": (pendulum.instance(executed_run.end_date) if executed_run.end_date else pendulum.now()) if executed_run else None,
        "run_duration": ( (pendulum.instance(executed_run.end_date) if executed_run.end_date else pendulum.now())
                         - pendulum.instance(executed_run.start_date)
                        ) if executed_run else None,
        "execution_data_interval": pendulum.interval(pendulum.instance(executed_run.data_interval_start),
                                                     pendulum.instance(executed_run.data_interval_end)
                                                    ) if executed_run else None,
    }

def _make_calendar_event(dagrun_detail):
    return {
        "title": dagrun_detail["display_name"] if "display_name" in dagrun_detail and dagrun_detail["display_name"] 
                    else (dagrun_detail["dag_id"] if "dag_id" in dagrun_detail else 'N/A'),
        "start": dagrun_detail["run_at"].set(microsecond=0).isoformat() if "run_at" in dagrun_detail and dagrun_detail["run_at"] else 'N/A',
        "end": (  dagrun_detail["run_at"] 
                + (dagrun_detail["run_duration"] if "run_duration" in dagrun_detail and dagrun_detail["run_duration"]
                                                else pendulum.interval(pendulum.now().set(second=0, microsecond=0), pendulum.now().set(second=0, microsecond=0)))
               ).set(microsecond=0).isoformat() 
                if "run_at" in dagrun_detail and dagrun_detail["run_at"] else 'N/A',
        "backgroundColor": dagrun_detail["dag_color"] if "dag_color" in dagrun_detail and dagrun_detail["dag_color"] else '#FFFFFF',
        "borderColor": get_border_color(dagrun_detail["run_state"]) if "run_state" in dagrun_detail else '#FFFFFF',
        "borderWidth": "3px",
        "extendedProps": {
            "status": dagrun_detail["run_state"] if "run_state" in dagrun_detail and dagrun_detail["run_state"] else 'no run',
            "cron": ( (  dagrun_detail["schedule"] + ' (' 
                       + (dagrun_detail["timezone"].name if "timezone" in dagrun_detail and dagrun_detail["timezone"] else 'UTC') 
                       + ')'
                      ) if isinstance(dagrun_detail["schedule"], str) else str(dagrun_detail["schedule"]) 
                    ) if "schedule" in dagrun_detail else 'N/A',
            "duration": (str(dagrun_detail["run_duration"].minutes) + 'm ' + str(dagrun_detail["run_duration"].remaining_seconds) + 's') 
                        if "run_duration" in dagrun_detail and dagrun_detail["run_duration"] else '300s',
            "dag_id": dagrun_detail["dag_id"] if "dag_id" in dagrun_detail else 'N/A',
            "task_count": dagrun_detail["task_count"] if "task_count" in dagrun_detail else 'N/A',
            "history": [],
        },
    }

def build_calendar_events(session, dagbag):
    events = []

    # prepare timestamp for data lookup
    now = pendulum.now("UTC")
    # TODO: read timespan from configuration
    start_search = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_search = (now + timedelta(days=8)).replace(hour=0, minute=0, second=0, microsecond=0)

    # get DAGs' colors
    dag_colors = load_dag_colors()

    # get the DAGBag content
    dagbag.collect_dags_from_db()

    # loop through DAGs
    for dag_id in dagbag.dag_ids:
        # get the DAG
        dag = dagbag.get_dag(dag_id, session)
        if dag and dag.get_is_active(session) and dag.get_is_paused(session) == False:
            # get DAG's scheduled runs for our timespan
            dagruns_scheduled = dag.iter_dagrun_infos_between(start_search, end_search)
            # get DAG's executed runs for our timespan
            dagruns_executed = dag.get_dagruns_between(start_search, end_search, session)

            # now we can combine the scheduled runs and actual runs
            #  * obviously only the past schedules have a run
            #  * and there can be a manual run outside of the schedule

            # let's prepare a lookup dictionaries to merge the runs
            scheduled_by_date = {pendulum.instance(dagrun_scheduled.logical_date): dagrun_scheduled for dagrun_scheduled in dagruns_scheduled}
            executed_by_date = {pendulum.instance(dagrun_executed.execution_date): dagrun_executed for dagrun_executed in dagruns_executed}
            # and also a list of dates
            dag_dates = set(scheduled_by_date) | set(executed_by_date)

            # now merge them (FULL OUTER JOIN) and create calendar events
            for dag_date in dag_dates:
                events.append(
                    _make_calendar_event(
                        _build_dagrun_detail(
                            dag, dag_date, 
                            scheduled_by_date.get(dag_date), executed_by_date.get(dag_date), 
                            get_dag_color(dag.dag_id, dag_colors)
                        )
                    )
                )

    return events
