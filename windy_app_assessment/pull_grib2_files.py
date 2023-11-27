import asyncio
import os
import re
from bz2 import BZ2Decompressor
from typing import List
from pathlib import Path
from datetime import datetime, timedelta

import aiorun
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from environs import Env

env = Env()
env.read_env()

GRIB2_FILES_DIRECTORY_URL = env.str("GRIB2_FILES_DIRECTORY_URL")
RESULT_DIR_PATH = Path(__file__).parent.parent.absolute() / "result"
ICON_D2_DIR_PATH = RESULT_DIR_PATH / "icon_d2"


def get_wgf4_output_dir_name(grib2_file_path: Path) -> str:
    """
    The result of GRIB2 files processing is the specific directory structure and PRATE.wgf4 files inside. This function
    generates the directory name for a given GRIB2 file.

    :param grib2_file_path: Path object representing a path to given GRIB2 file
    :return: a directory name where WGF4 file would be created
    """
    year, month, day, hour, offset = map(
        int,
        re.match(r".*(\d{4})(\d{2})(\d{2})(\d{2})_(\d{3})_2d", str(grib2_file_path)).groups()
    )
    # TODO (dmitry): have a constant for date/time format
    return (datetime(year, month, day, hour) + timedelta(hours=offset)).strftime("%d.%m.%Y_%H:%M_%s")



async def process_grib2_files(grib2_files_directory_path: Path) -> None:
    for grib2_file_path in sorted(grib2_files_directory_path.iterdir()):
        # create directory for current hour
        # TODO (dmitry): if a directory exists, we might already have the data there, so ideally we shouldn't prcess
        # current GRIB2 file in this case
        # but here we'll just ignore that directory exists, the new WGF4 file would be created and stored in this dir
        (ICON_D2_DIR_PATH / get_wgf4_output_dir_name(grib2_file_path)).mkdir(exist_ok=True)
        # prepare PRATE.wgf4 header


async def download_grib2_file(
        session: aiohttp.ClientSession,
        grib2_files_directory_path: Path,
        grib2_file_url: str
) -> None:
    """
    Download given GRIB2 files to temporary directory for further processing.

    NOTES:
    * at the moment we expect bzip2 encoded files;
    * all the files would be saved to temporary directory

    :param session: aiohttp session
    :param grib2_files_directory_path: Path object representing a path to temporary directory with GRIB2 files
    :param grib2_file_url: URL to (bzip2 compressed) GRIB2 file
    """
    async with session.get(grib2_file_url) as resp:
        # TODO (dmitry): define the action in case of HTTP error code
        if resp.status == 200:
            file_name = grib2_file_url.split("/")[-1][:-4]
            # TODO (dmitry): consider in-memory files to reduce time spent on disk i/o operations
            async with aiofiles.open(grib2_files_directory_path / file_name, "wb") as decompressed_file:
                decompressor = BZ2Decompressor()
                async for data in resp.content.iter_any():
                    await decompressed_file.write(decompressor.decompress(data))


async def get_grib2_files_urls(session: aiohttp.ClientSession, grib2_files_directory_url: str) -> List[str]:
    """
    Returns the list of GRIB2 files URL found on the directory page

    :param session: shared aiohttp session
    :param grib2_files_directory_url: URL of "directory" page with the list of grib2 files

    :return: The list of GRIB2 files URL
    """
    async with session.get(grib2_files_directory_url) as resp:
        page_content: str = await resp.text()
        return [
            # we expect that the files are located under the "directory" page URL; it might be wrong for other
            # data providers
            grib2_files_directory_url + "/" + node.get("href")
            for node in BeautifulSoup(page_content, "html.parser").find_all("a")
            # TODO (dmitry): the program should be ready for simple `.grib2` files on the page (or other thing)
            # TODO (dmitry): in this specific case we're interested in the files with `regular-lat-lon` string in the name
            # it might be wrong for other data providers
                   if node.get("href").endswith("grib2.bz2") and "regular-lat-lon" in node.get("href")
        ]


async def async_main():
    # TODO (dmitry): replace print statements with logging
    print("Starting")

    async with aiohttp.ClientSession() as session:
        async with aiofiles.tempfile.TemporaryDirectory() as grib2_files_directory:
            grib2_files_directory_path: Path = Path(grib2_files_directory)
            await asyncio.gather(*([
                download_grib2_file(session, grib2_files_directory_path, grib2_file_url)
                for grib2_file_url in await get_grib2_files_urls(session, GRIB2_FILES_DIRECTORY_URL)
            ]))
            await process_grib2_files(grib2_files_directory_path)

    print("All done")


def main() -> None:
    aiorun.run(async_main(), stop_on_unhandled_errors=True)


if __name__ == "__main__":
    main()