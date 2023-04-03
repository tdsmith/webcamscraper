from collections.abc import Collection, Mapping
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import NoReturn
import datetime as dt
import logging

import aiobotocore.session
import attr
import cattr
import httpx
import structlog
import trio
import trio_asyncio
import tomllib
import typer
from toolz import groupby
from types_aiobotocore_s3 import S3Client

logging.basicConfig(
    level=logging.INFO,
)

logger = structlog.get_logger()


@attr.define
class VisitTask:
    url: str
    visit_interval_sec: int
    filename_template: str
    last_modified: str | None = None
    etag: str | None = None
    last: bytes | None = None

    def format_filename(self, last_modified: dt.datetime) -> str:
        return self.filename_template.format(
            last_modified=last_modified.strftime("%Y-%m-%d-%H%M%S")
        )


@attr.frozen
class S3Config:
    aws_secret_access_key: str
    aws_access_key_id: str
    aws_s3_endpoint_url: str
    bucket: str


@attr.frozen
class Context:
    s3_client: S3Client
    s3_config: S3Config
    http: httpx.AsyncClient


@attr.frozen
class FetchResponse:
    content: bytes
    headers: Mapping[str, str]


async def do_fetch(http: httpx.AsyncClient, task: VisitTask) -> FetchResponse | None:
    headers = {}
    if task.last_modified:
        headers["If-Modified-Since"] = task.last_modified
    if task.etag:
        headers["If-None-Match"] = task.etag

    response = await http.get(task.url, headers=headers)
    if response.status_code == 200:
        return FetchResponse(response.content, response.headers)
    elif response.status_code == 304:
        return
    else:
        response.raise_for_status()
        assert False


async def handle_task(ctx: Context, task: VisitTask) -> None:
    with trio.move_on_after(90) as attempt:
        try:
            fetched = await do_fetch(ctx.http, task)
        except Exception:
            logger.exception("do_fetch exception", url=task.url)
            return
    if attempt.cancelled_caught:
        logger.info("do_fetch timeout", url=task.url)
        return
    if (not fetched) or (task.last and (fetched.content == task.last)):
        logger.debug("do_fetch no change", url=task.url)
        return
    logger.debug("do_fetch succeeded", url=task.url)

    last_modified = (
        parsedate_to_datetime(fetched.headers["Last-Modified"])
        if "Last-Modified" in fetched.headers
        else dt.datetime.now(dt.timezone.utc)
    )
    filename = task.format_filename(last_modified)

    with trio.move_on_after(90) as attempt:
        try:
            await trio_asyncio.aio_as_trio(ctx.s3_client.put_object)(
                Bucket=ctx.s3_config.bucket,
                Key=filename,
                Body=fetched.content,
            )
        except Exception:
            logger.exception("handle_task put exception", url=task.url)
            return
    if attempt.cancelled_caught:
        logger.info("handle_task put timeout", url=task.url)
        return

    task.etag = fetched.headers.get("ETag")
    task.last_modified = fetched.headers.get("Last-Modified")
    if not ("ETag" in fetched.headers or "Last-Modified" in fetched.headers):
        task.last = fetched.content
    logger.info("handle_task succeeded", url=task.url)


async def spawn_tasks(
    interval_sec: float,
    ctx: Context,
    tasks: Collection[VisitTask],
) -> NoReturn:
    async with trio.open_nursery() as nursery:
        next_wake: float = trio.current_time() + interval_sec
        while True:
            for task in tasks:
                nursery.start_soon(handle_task, ctx, task)
            await trio.sleep_until(next_wake)
            next_wake = next_wake + interval_sec


async def daemon(s3_config: S3Config, tasks: Collection[VisitTask]) -> NoReturn:
    by_interval = groupby(lambda task: task.visit_interval_sec, tasks)
    session = aiobotocore.session.get_session()
    async with trio_asyncio.aio_as_trio(
        session.create_client(
            "s3",
            aws_secret_access_key=s3_config.aws_secret_access_key,
            aws_access_key_id=s3_config.aws_access_key_id,
            endpoint_url=s3_config.aws_s3_endpoint_url,
        )
    ) as s3_client:
        async with httpx.AsyncClient(http2=True) as http:
            ctx = Context(s3_client, s3_config, http)
            async with trio.open_nursery() as nursery:
                for interval, tasks in by_interval.items():
                    logger.info(
                        "starting main loop", interval=interval, n_tasks=len(tasks)
                    )
                    nursery.start_soon(spawn_tasks, interval, ctx, tasks)
    raise RuntimeError("spawn_tasks terminated?")


def main(
    cameras_toml: Path,
    secrets_toml: Path,
) -> None:
    with cameras_toml.open("rb") as f:
        d = tomllib.load(f)
    tasks = cattr.structure(d, dict[str, list[VisitTask]])["cameras"]
    with secrets_toml.open("rb") as f:
        secrets = tomllib.load(f)
    s3_config = S3Config(**secrets)
    trio_asyncio.run(daemon, s3_config, tasks)


if __name__ == "__main__":
    typer.run(main)
