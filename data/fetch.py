import re
from urllib.parse import urljoin

import attr
import cattrs
import bs4
import httpx
import trio
import tomli_w
import typer

@attr.define(order=True)
class WebcamFields:
    mapid: str
    name: str
    url: str
    geo_point_2d: tuple[float, float]
    geo_local_area: str = "GVRD"
    image_urls: list[str] | None = None

async def scrape_pictures_from(client: httpx.AsyncClient, camera: WebcamFields) -> None:
    print(camera.name)
    result = await client.get(camera.url, follow_redirects=True)
    result.raise_for_status()
    cams = bs4.BeautifulSoup(result.content, "html.parser").find_all("img", src=re.compile(".*jpg"))
    cam_urls = [
        urljoin(camera.url, cam["src"])
        for cam in cams
    ]
    camera.image_urls = [cam_url for cam_url in cam_urls if re.search(r"(pub/cameras/\d)|(/cameraimages/)", cam_url)]

async def visit_and_extract_from(webcam_url) -> list[WebcamFields]:
    async with httpx.AsyncClient() as client:
        webcams = await client.get(webcam_url)
    records = webcams.json()["records"]
    cameras = [cattrs.structure(record["fields"], WebcamFields) for record in records]
    cameras = [camera for camera in cameras if camera.geo_local_area in ("GVRD", "Downtown", "West End")]
    async with httpx.AsyncClient() as client:
        async with trio.open_nursery() as nursery:
            for camera in cameras:
                nursery.start_soon(
                    scrape_pictures_from, client, camera
                )
    return cameras


def main(output: typer.FileTextWrite = typer.Option("-")):
    response = trio.run(
        visit_and_extract_from,
        "https://opendata.vancouver.ca/api/records/1.0/search/?dataset=web-cam-url-links&q=&rows=1000",
    )
    output.write(tomli_w.dumps(dict(cameras=cattrs.unstructure(response))))


if __name__ == "__main__":
    typer.run(main)
