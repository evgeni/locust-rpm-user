import random
import bz2
import gzip
import lzma
import xml.etree.ElementTree as ET

from typing import Iterator

from locust import FastHttpUser, task  # type: ignore


class RPMUser(FastHttpUser):
    def fetch_packages_list_from_repo(self) -> Iterator[str]:
        r = self.client.get("/repodata/repomd.xml")

        repomd = ET.fromstring(r.content)

        primary_location = repomd.find('.//*[@type="primary"]/{http://linux.duke.edu/metadata/repo}location').get("href")
        if primary_location.endswith(".xz"):
            decompress = lzma.decompress
        elif primary_location.endswith(".bz2"):
            decompress = bz2.decompress
        elif primary_location.endswith(".gz"):
            decompress = gzip.decompress
        else:
            raise ValueError(primary_location)

        s = self.client.get(f"/{primary_location}")

        primary = ET.fromstring(decompress(s.content))

        for pkg in primary.iter("{http://linux.duke.edu/metadata/common}package"):
            location = pkg.find("{http://linux.duke.edu/metadata/common}location").get("href")
            yield f"/{location}"

    def on_start(self) -> None:
        self.urls = list(self.fetch_packages_list_from_repo())


class RandomRPMUser(RPMUser):
    @task
    def random_rpm(self) -> None:
        url = random.choice(self.urls)
        self.client.get(url)


class AllRPMUser(RPMUser):
    @task
    def all_rpm(self) -> None:
        for url in self.urls:
            self.client.get(url)
