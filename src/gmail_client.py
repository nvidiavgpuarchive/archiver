import asyncio
import email
from email.header import decode_header
from typing import List, Tuple

import aioimaplib

import app_config
import utils
from logger import get_logger

_logger = get_logger(__name__)


class GmailClient:
    """
    IMAPClient to fetch list of emails and read them, for verification codes
    Althought this class is async compatible, only one task can be done at a time
    due to the limitations of imap protocals
    """

    def __init__(self, host, port, username, password):
        self._host = host
        self._port = port
        self._username = username
        self._password = password

        self._imap_client = None

        self._mailboxes = None
        self._junkbox_name = None
        self._trashbox_name = None

    async def connect(self):
        """
        If not connected, connect and login to gmail server.
        It's not a must to call this function explicitly, all other methods
        will call this first to ensure logged in.
        """
        if self._imap_client is not None and await self._is_connected():
            return
        self._imap_client = (
            (aioimaplib.IMAP4_SSL(self._host))
            if self._port == 993
            else aioimaplib.IMAP4(self._host, self._port)
        )
        await self._imap_client.wait_hello_from_server()
        await self._imap_client.login(self._username, self._password)

        # cache mailboxes to speedup future use
        if not self._mailboxes:
            status, mailboxes = await self._imap_client.list('""', "*")
            mailboxes = [i.decode("utf-8") for i in mailboxes]

            mailbox_list = {}
            for mailbox in mailboxes:
                name = mailbox.split(" ")[-1].strip('"')
                attributes = []
                for attr in (
                    mailbox.split('"/"')[0]
                    .strip(" ")
                    .lstrip("(")
                    .rstrip(")")
                    .split(" ")
                ):
                    attributes.append(attr.lstrip("\\").lower())
                mailbox_list[name] = set(attributes)

            self._mailboxes = mailbox_list
            self._trashbox_name = [k for k, v in mailbox_list.items() if "trash" in v][
                0
            ]
            self._junkbox_name = [k for k, v in mailbox_list.items() if "junk" in v][0]

        _logger.info(f"Logged in to IMAP server '{self._host}:{self._port}'")

    async def _is_connected(self):
        try:
            await self._imap_client.select("inbox")
            return True
        except Exception as e:
            return False

    async def search_for(self, sender: str, unseen=True) -> List[Tuple[str, str]]:
        await self.connect()

        result = []
        for mailbox in ["INBOX", self._junkbox_name]:
            await self._imap_client.select(mailbox)
            criteria = ("FROM", sender) if not unseen else ("FROM", sender, "UNSEEN")
            _, data = await self._imap_client.search(*criteria)
            result += [(mailbox, eid) for eid in data[0].split()]
        return result

    async def delete_mail(self, eid: str, mailbox="INBOX"):
        await self.connect()
        await self._imap_client.select(mailbox)
        await self._imap_client.copy(eid, self._trashbox_name)
        await self._imap_client.store(eid, "+FLAGS", "\\Deleted")
        await self._imap_client.expunge()
        _logger.info(f"Email {eid} trashed from {mailbox}.")

    async def get_mail(self, eid, mailbox="INBOX"):
        """
        Fetch a single email by eid and return its relevant information.
        """
        await self.connect()
        await self._imap_client.select(mailbox)

        eid = eid.decode() if isinstance(eid, bytes) else eid
        status, msg_data = await self._imap_client.fetch(eid, "(RFC822)")
        if status != "OK":
            utils.log_error_and_raise(
                _logger, f"Cannot fetch email {eid} from {mailbox}"
            )

        msg = email.message_from_bytes(msg_data[1])
        # Subject
        subj_raw, subj_enc = decode_header(msg.get("Subject"))[0]
        subject = (
            subj_raw.decode(subj_enc or "utf-8", errors="replace")
            if isinstance(subj_raw, bytes)
            else (subj_raw or "")
        )
        # Sender
        from_raw, from_enc = decode_header(msg.get("From"))[0]
        sender = (
            from_raw.decode(from_enc or "utf-8", errors="replace")
            if isinstance(from_raw, bytes)
            else (from_raw or "")
        )
        # Recipients
        to_list = email.utils.getaddresses(msg.get_all("To", []))
        recipients = [addr for name, addr in to_list]
        # Date
        date_str = msg.get("Date")
        date_val = None
        if date_str:
            try:
                date_val = email.utils.parsedate_to_datetime(date_str)
            except Exception:
                date_val = None
        # Body (plain text)
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                disp = str(part.get("Content-Disposition"))
                if ctype == "text/plain" and "attachment" not in disp:
                    try:
                        part_content = part.get_payload(decode=True)
                        charset = part.get_content_charset() or "utf-8"
                        chunk = part_content.decode(charset, errors="replace")
                        body += chunk
                    except Exception:
                        continue
        else:
            try:
                content = msg.get_payload(decode=True)
                charset = msg.get_content_charset() or "utf-8"
                body = content.decode(charset, errors="replace")
            except Exception:
                body = ""

        return {
            "eid": eid,
            "mailbox": mailbox,
            "subject": subject,
            "sender": sender,
            "recipients": recipients,
            "time": date_val.timestamp(),
            "body": body.strip(),
        }


# test email out
if __name__ == "__main__":

    async def main():
        config = app_config.load_config()
        gmail_client = GmailClient(
            config.imap.host,
            config.imap.port,
            config.imap.username,
            config.imap.password,
        )
        await gmail_client.connect()
        eids = await gmail_client.search_for("account@nvidia.com", unseen=False)
        for mailbox, eid in eids:
            data = await gmail_client.get_mail(eid, mailbox=mailbox)
            data["body"] = None

    asyncio.run(main())
