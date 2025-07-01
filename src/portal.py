# login into licensing portal using selenium,
# dump cookies, and list available downloads
import asyncio
import atexit
import json
import os
import re
import time
from functools import partial
from typing import Dict, List, Optional, TypedDict

import aiofiles
import aiohttp
from playwright.async_api import BrowserContext, Page, TimeoutError, async_playwright
from yarl import URL

import utils
from gmail_client import GmailClient
from logger import get_logger

_logger = get_logger(__name__)


class MetaInfo(TypedDict):
    linkType: str
    category: str
    downloadType: str
    downloadId: str
    name: str
    productName: str
    releaseDate: str
    version: str
    platformVersion: str
    productFamilies: list[str]
    platformName: str
    checksumFormat: str
    description: str


def same_meta(meta1: MetaInfo, meta2: MetaInfo):
    return (
        meta1["downloadId"] == meta2["downloadId"]
        or meta1["description"] == meta2["description"]
    )


class DownloadInfo(TypedDict):
    id: str
    url: str
    checksumUrl: str


class NvidiaWebPortal:
    """
    Among all public accessable methods, login should not run concurrently

    """

    def __init__(
        self,
        username: str,
        password: str,
        gmail_client: GmailClient,
        https_proxy: str | None = None,
        remote_playwright_link: str | None = None,
    ):
        self._https_proxy = https_proxy
        self._username = username
        self._password = password
        self._gmail_client = gmail_client
        self._remote_playwright_link = remote_playwright_link

        self._session: Optional[aiohttp.ClientSession] = None
        self._userinfo: Optional[Dict] = None
        self._virtual_groups: Optional[Dict] = None

    async def list_meta(self) -> Optional[List[MetaInfo]]:
        orgname = self._userinfo["user"]["orgName"]
        vgroup_id = self._virtual_groups["virtualGroups"][0]["id"]
        url = (
            f"https://api.licensing.nvidia.com/v1/org/{orgname}/virtual-groups/"
            f"{vgroup_id}/downloads"
        )
        data = {"downloadsFetch": {}}

        async with self._session.post(url, json=data, proxy=self._https_proxy) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                return json_data["downloads"]
            else:
                utils.log_error_and_raise(
                    _logger,
                    f"List downloads request failed with status " f"" f"{resp.status}",
                )
                return None

    async def get_download_url(self, download_id: str) -> Optional[DownloadInfo]:
        orgname = self._userinfo["user"]["orgName"]
        vgroup_id = self._virtual_groups["virtualGroups"][0]["id"]
        url = (
            f"https://api.licensing.nvidia.com/v1/org/{orgname}/virtual-"
            f"groups/{vgroup_id}/download/url"
        )
        data = {"downloadId": [download_id]}

        async with self._session.post(url, json=data, proxy=self._https_proxy) as resp:
            if resp.status == 200:
                json_data = await resp.json()
                return json_data["downloadUrls"][0]
            else:
                utils.log_error_and_raise(
                    _logger,
                    f"Get download url request failed with status "
                    f""
                    f"{resp.status}",
                )

    async def is_loggedin(self) -> bool:
        try:
            await self._update_userinfo()
            return True
        except:
            return False

    async def _update_userinfo(self):
        """
        Update userinfo and virtual groups.
        Also serves as a healthcheck, returns True if both fetches succeed.
        """
        userinfo_url = "https://api.licensing.nvidia.com/v1/users/me"
        vgroups_url_tmpl = (
            "https://api.licensing.nvidia.com/v1/org/{orgname}/virtual-groups"
        )
        async with self._session.get(userinfo_url, proxy=self._https_proxy) as resp:
            if resp.status == 200:
                info = await resp.json()
                orgname = info["user"]["orgName"]
                async with self._session.get(
                    vgroups_url_tmpl.format(orgname=orgname), proxy=self._https_proxy
                ) as v_resp:
                    if v_resp.status == 200:
                        self._userinfo = info
                        self._virtual_groups = await v_resp.json()
                        return
                    utils.log_error_and_raise(
                        _logger,
                        f"Get virtual groups request failed with status "
                        f"{v_resp.status}",
                    )
            utils.log_error_and_raise(
                _logger,
                f"Update userinfo request failed with status {resp.status}",
            )

    async def login(self, debug=False):
        login_url = "https://nvid.nvidia.com/login"
        _logger.info("Authenticating to Nvidia...")

        if self._https_proxy and (
            (
                not self._https_proxy.startswith("https://")
                and not self._https_proxy.startswith("http://")
            )
            or ":" not in self._https_proxy
        ):
            _logger.error("https proxy must in format 'http(s)://<host>:<port>'")
            utils.log_error_and_raise(
                _logger, f"Invalid proxy format {self._https_proxy}"
            )
        proxy_options = None if not self._https_proxy else {"server": self._https_proxy}

        async with async_playwright() as p:
            if self._remote_playwright_link:
                browser = await p.chromium.connect(self._remote_playwright_link)
                _logger.info(
                    f"Connected to remote playwright instance "
                    f"{self._remote_playwright_link}"
                )
            else:
                browser = await p.chromium.launch(
                    headless=not debug, proxy=proxy_options
                )
                _logger.info("Launched local playwright instance.")

            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(login_url, wait_until="networkidle")

            # cookies button
            try:
                await page.get_by_role("button", name="Accept All").click(timeout=1000)
                await asyncio.sleep(1)
            except Exception:
                _logger.debug("No cookies button, skip.")

            # email and password
            await page.get_by_role("textbox", name="please enter email").fill(
                self._username
            )
            await page.get_by_role("button", name="Sign In").click()
            await page.wait_for_url("https://login.nvgs.nvidia.com/v1/login/password**")
            await page.get_by_role("textbox", name="Password").fill(self._password)
            await page.get_by_role("button", name="Log In").click()
            _logger.debug("Proceed to the last step of login.")

            # try login in a loop
            # TODO: Nvidia asks for 2fa sometimes. Add it here
            urls = {
                "success": "https://ui.licensing.nvidia.com",
                "email_verification": "https://login.nvgs.nvidia.com/v1/nfactor/email-auth-wait"
                "**",
            }
            for _ in range(3):
                try:
                    tag = await playwright_wait_for_any(page, urls, timeout=30)
                except Exception:
                    utils.log_error_and_raise(_logger, "Timeout on last step of login.")

                if tag == "success":
                    try:
                        await page.wait_for_selector(
                            "span.button-text", state="visible"
                        )
                        _logger.info("Successfully logged in.")
                        break
                    except Exception:
                        continue

                elif tag == "email_verification":
                    _logger.info("Email verification required.")
                    try:
                        link = await self._wait_for_verification_link()
                    except Exception:
                        _logger.warning("Email verification timeout. Try again anyway.")
                        continue

                    page2 = await context.new_page()
                    await page2.goto(link)
                    await asyncio.sleep(10)
                    await page2.close()
                    _logger.info("Email verification cleared.")

                await page.bring_to_front()
                _logger.info("Proceed.")

            await self._load_session_from_context(context)
            await self._update_userinfo()
            await page.close()
            await context.close()

            try:
                sub_end_date = self._virtual_groups["virtualGroups"][0]["entitlements"][
                    0
                ]["entitlementProductKeys"][0]["entitlementFeatures"][0]["endDate"]
                _logger.info("Current subscription ends at %s", sub_end_date)
            except (KeyError, IndexError):
                _logger.warning("Cannot get virtual groups entitlements ending date.")

            if debug:
                input("Press Enter to continue...")

        _logger.info("Playright quitted.")

    async def _wait_for_verification_link(self, timeout=60) -> str:
        """
        Wait for verification email, using gmail client
        Returns verification code. Or raise timeout exception.
        """

        nvidia_sender_email = "account@nvidia.com"
        nvidia_veri_link_re = (
            r"https://accounts\.nvgs\.nvidia\.com/api/1/message/VerifyEmail\?q"
            r"=[\w\.\-]+"
        )

        _logger.info(f"Waiting for verification email up to {timeout} seconds.")

        start_time = time.time()  # we need real time
        while time.time() < start_time + timeout:
            await asyncio.sleep(1)
            eid_tuples = await self._gmail_client.search_for(nvidia_sender_email)
            mail_data_list = [
                await self._gmail_client.get_mail(eid, mailbox)
                for mailbox, eid in eid_tuples
            ]
            mail_data_list.sort(key=lambda x: x["time"], reverse=True)  # newest first

            for mail_data in mail_data_list:
                if mail_data["time"] < start_time - 60:
                    continue

                matches = re.findall(nvidia_veri_link_re, mail_data["body"])
                if matches == []:
                    _logger.debug("No verification link found, skip.")
                    continue

                # matched, delete the verification email and return
                _logger.info(f"Verification email received.")
                _logger.debug(matches[0])
                await self._gmail_client.delete_mail(
                    mail_data["eid"], mailbox=mail_data["mailbox"]
                )
                return matches[0]

        _logger.error("Timeout waiting for verification email.")
        raise TimeoutError("Timeout waiting for verification email.")

    async def _load_session_from_context(self, context: BrowserContext):
        current_page = context.pages[-1]
        page = await context.new_page()
        await page.goto("https://api.licensing.nvidia.com/v1/users/me")
        await page.close()
        await current_page.bring_to_front()
        playwright_cookies = await context.cookies()
        async with aiofiles.open(utils.proj_path("config/cookies.json"), "w") as f:
            await f.write(json.dumps(playwright_cookies))

        await self.load_session_from_cookies()

    async def load_session_from_cookies(self):
        if not os.path.exists(utils.proj_path("config/cookies.json")):
            return
        async with aiofiles.open(utils.proj_path("config/cookies.json"), "r") as f:
            playwright_cookies = json.loads(await f.read())

        session = aiohttp.ClientSession()
        atexit.register(partial(utils.run_async_blocking, session.close))

        jar = session.cookie_jar
        for cookie in playwright_cookies:
            if "domain" in cookie:
                jar.update_cookies(
                    {cookie["name"]: cookie["value"]},
                    response_url=URL(f"http://{cookie['domain']}"),
                )
            else:
                jar.update_cookies({cookie["name"]: cookie["value"]})

        self._session = session


async def playwright_wait_for_any(page: Page, urls: Dict[str, str], timeout=30):
    """
    Wait for any of the urls to be loaded, return when any of them is loaded.
    Raise timeout if all timeout
    """

    tasks = {
        asyncio.create_task(page.wait_for_url(url, timeout=timeout * 1000)): tag
        for tag, url in urls.items()
    }

    done, pending = await asyncio.wait(
        tasks.keys(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    for finished_task in done:
        if finished_task.exception() is None:
            return tasks[finished_task]
    raise TimeoutError("Timeout, no url matches the criteria.")


async def main():
    config = utils.read_config()
    gmail_client = GmailClient(
        config["imap"]["host"],
        config["imap"]["port"],
        config["imap"]["username"],
        config["imap"]["password"],
    )

    portal = NvidiaWebPortal(
        username=config["portal"]["nvidia_username"],
        password=config["portal"]["nvidia_password"],
        https_proxy=config["global"]["https_proxy"],
        gmail_client=gmail_client,
    )

    connect_task = asyncio.create_task(gmail_client.connect())
    await portal.login(debug=False)

    metas = await portal.list_meta()

    async def download_info_worker(taskid, meta: MetaInfo, semaphore):
        async with semaphore:
            _logger.info(f"{taskid} Fetching '{meta["description"]}'")
            for _ in range(3):
                try:
                    download = await portal.get_download_url(meta["downloadId"])
                except:
                    continue

            return download

    semaphore = asyncio.Semaphore(32)
    downloads = await asyncio.gather(
        *(
            download_info_worker(taskid, meta, semaphore)
            for taskid, meta in enumerate(metas)
        )
    )
    with open(utils.proj_path("config/downloads.json"), "w") as f:
        f.write(json.dumps(downloads, indent=4, default=str))


if __name__ == "__main__":
    asyncio.run(main())
