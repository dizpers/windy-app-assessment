import asyncio
from bz2 import BZ2Decompressor
from typing import List

import aiorun
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from environs import Env

env = Env()
env.read_env()

GRIB2_FILES_DIRECTORY_URL = env.str("GRIB2_FILES_DIRECTORY_URL")


async def download_and_process_grib2_file(session: aiohttp.ClientSession, grib2_file_url: str) -> None:
    """
    Download given GRIB2 file and process it. NOTE: at the moment we expect
    bzip2 encoded files.

    :param session: aiohttp session
    :param grib2_file_url: URL to (bzip2 compressed) GRIB2 file
    """
    async with session.get(grib2_file_url) as resp:
        # TODO (dmitry): retry in case of error? logging?
        if resp.status == 200:
            async with aiofiles.open("test.grib2", "wb") as decompressed_file:
                decompressor = BZ2Decompressor()
                async for data in resp.content.iter_any():
                    await decompressed_file.write(decompressor.decompress(data))


async def get_grib2_files_urls(session: aiohttp.ClientSession, grib2_files_directory_url: str) -> List[str]:
    """
    Returns the list of GRIB2 URLs found on the directory page

    :param session: aiohttp session
    :param grib2_files_directory_url: URL of the directory with grib2 files

    :return: The list of GRIB2 file URLs
    """
    async with session.get(grib2_files_directory_url) as resp:
        page_content: str = await resp.text()
        return [
            grib2_files_directory_url + "/" + node.get("href")
            for node in BeautifulSoup(page_content, "html.parser").find_all("a") if node.get("href").endswith("grib2.bz2")
        ][:1]

async def async_main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*([
            download_and_process_grib2_file(session, grib2_file_url)
            for grib2_file_url in await get_grib2_files_urls(session, GRIB2_FILES_DIRECTORY_URL)
        ]))

    print("all done")


def main() -> None:
    aiorun.run(async_main(), stop_on_unhandled_errors=True)


if __name__ == "__main__":
    main()