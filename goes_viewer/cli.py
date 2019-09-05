from concurrent.futures import ProcessPoolExecutor
import datetime as dt
from functools import partial, wraps
import json
import logging
import os
from pathlib import Path
import sys
import time


import click
from croniter import croniter
import pytz


from goes_viewer import __version__


def handle_exception(logger, exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception",
                 exc_info=(exc_type, exc_value, exc_traceback))


def basic_logging_config():
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s %(levelname)s %(message)s')
    sentry_dsn = os.getenv('SENTRY_DSN', None)
    if sentry_dsn is not None:
        import sentry_sdk
        sentry_sdk.init(dsn=sentry_dsn, release=f'goes_viewer@{__version__}')


sys.excepthook = partial(handle_exception, logging.getLogger())
basic_logging_config()
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def set_log_level(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        verbose = kwargs.pop("verbose", 0)
        if verbose == 1:
            loglevel = "INFO"
        elif verbose > 1:
            loglevel = "DEBUG"
        else:
            loglevel = "WARNING"
        logging.getLogger().setLevel(loglevel)
        return f(*args, **kwargs)

    return wrapper


def set_user_pass(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    with open(value, "r") as f:
        user = f.readline().strip("\n")
        passwd = f.readline().strip("\n")
    os.environ["APIUSER"] = user
    os.environ["APIPASS"] = passwd


verbose = click.option("-v", "--verbose", count=True, help="Increase logging verbosity")
timeout = click.option(
    "--timeout", show_default=True, default=30, help="Request timeout"
)


def common_options(cmd):
    """Combine common options into one decorator"""

    def wrapper(f):
        decs = [
            verbose,
            click.option(
                "-u",
                "--user",
                show_envvar=True,
                help="Username to access API.",
                envvar="APIUSER",
                required=True,
            ),
            click.option(
                "-p",
                "--password",
                show_envvar=True,
                envvar="APIPASS",
                required=True,
                prompt=True,
                hide_input=True,
                help="Password to access API",
            ),
            click.option(
                "--passfile",
                help="File containing username and password on separate lines. Overrides env variables",  # NOQA
                callback=set_user_pass,
                is_eager=True,
                expose_value=False,
            ),
            timeout,
            click.argument("base_url"),
        ]
        for dec in reversed(decs):
            f = dec(f)
        return f

    return wrapper(cmd)


def schedule_options(cmd):
    """Combine scheduling options into one decorator"""

    def wrapper(f):
        decs = [
            click.option("--cron", help="Run the script on a cron schedule"),
            click.option(
                "--cron-tz",
                help="Timezone to use for cron scheduling",
                show_default=True,
                default="UTC",
            ),
        ]
        for dec in reversed(decs):
            f = dec(f)
        return f

    return wrapper(cmd)


def _now(tz):
    return dt.datetime.now(tz=pytz.timezone(tz))


def run_times(cron, cron_tz):
    now = _now(cron_tz)
    iter = croniter(cron, now)
    while True:
        next_time = iter.get_next(dt.datetime)
        if next_time > _now(cron_tz):
            yield next_time


def silent_exit(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            out = f(*args, **kwargs)
        except (SystemExit, KeyboardInterrupt):
            pass
        else:
            return out

    return wrapper


@silent_exit
def run_loop(fnc, *args, cron, cron_tz, **kwargs):
    if cron is None:
        return fnc(*args, **kwargs)
    for rt in run_times(cron, cron_tz):
        sleep_length = (rt - _now(cron_tz)).total_seconds()
        logging.info("Sleeping for %0.1f s to next run time at %s", sleep_length, rt)
        if sleep_length > 0:
            time.sleep(sleep_length)
        with ProcessPoolExecutor(1) as exc:
            fut = exc.submit(fnc, *args, **kwargs)
            fut.result()


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__)
def cli():
    """
    The goes_viewer command line tool. At this time config.py may load
    environment variables that are of interest.
    """
    pass  # pragma: no cover


class PathParamType(click.Path):
    def convert(self, value, param, ctx):
        p = super().convert(value, param, ctx)
        return Path(p)


save_directory = click.argument(
    "save_directory",
    type=PathParamType(
        exists=True, writable=True, resolve_path=True, file_okay=False),
)


@cli.command()
@verbose
@schedule_options
@set_log_level
@click.argument('sqs_url')
@save_directory
def process_files(sqs_url, save_directory, cron, cron_tz):
    """
    Process new files in SQS_URL and save the GeoColor images to SAVE_DIRECTORY
    """
    from goes_viewer.process_files import get_process_and_save

    run_loop(
        get_process_and_save,
        sqs_url,
        save_directory,
        cron=cron,
        cron_tz=cron_tz
    )


class JSONParamType(click.ParamType):
    name = "json"

    def convert(self, value, param, ctx):
        return json.loads(value)


JSON = JSONParamType()
data_params = click.option(
    "-d",
    "--data-params",
    show_envvar=True,
    envvar="GV_DATA_PARAMS",
    default='{}',
    show_default=True,
    help="Extra parameters when retrieving data",
    type=JSON
)


@cli.command()
@common_options
@set_log_level
@click.option(
    "-f",
    "--filters",
    show_envvar=True,
    envvar="GV_FILTERS",
    default='{"Type": "ghi"}',
    show_default=True,
    help="Filters to apply to metadata",
    type=JSON
)
@data_params
@save_directory
def initialize(base_url, user, password, timeout, save_directory, filters,
               data_params):
    """
    Initialize the figure and metadata with data from BASE_URL and save
    to SAVE_DIRECTORY
    """
    from goes_viewer.write_metadata import parse_metadata
    from goes_viewer.figure import render_html

    meta = parse_metadata(base_url, filters, auth=(user, password),
                          params=data_params, timeout=timeout)
    with open(save_directory / "metadata.json", "w") as f:
        json.dump(meta, f)
    render_html(save_directory)


@cli.command()
@common_options
@schedule_options
@set_log_level
@data_params
@click.argument(
    'metadata_file',
    type=PathParamType(exists=True, file_okay=True, dir_okay=False,
                       writable=True, readable=True, resolve_path=True)
)
def update_values(base_url, user, password, timeout, metadata_file,
                  data_params, cron, cron_tz):
    """
    Update the METADATA_FILE with current values from BASE_URL
    """
    from goes_viewer.write_metadata import update_existing_file

    run_loop(
        update_existing_file,
        metadata_file,
        base_url,
        (user, password),
        data_params,
        timeout,
        cron=cron,
        cron_tz=cron_tz
    )


if __name__ == '__main__':
    cli()
