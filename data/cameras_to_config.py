import os.path
import tomllib
from urllib.parse import urlparse

import tomli_w
import typer


def main(
    input: typer.FileBinaryRead,
    output: typer.FileBinaryWrite = typer.Option("-", "--output", "-o"),
):
    out = {"cameras": []}
    cameras = tomllib.load(input)["cameras"]
    for camera in cameras:
        for image_url in camera["image_urls"]:
            parsed = urlparse(image_url)
            basename, *_ = os.path.splitext(os.path.basename(parsed.path))
            out["cameras"].append(
                dict(
                    url=image_url,
                    visit_interval_sec=90,
                    filename_template=f"webcams/{camera['mapid']}/{basename}_{{last_modified}}.jpg",
                )
            )
    tomli_w.dump(out, output)


if __name__ == "__main__":
    typer.run(main)
