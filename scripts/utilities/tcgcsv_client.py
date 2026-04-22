import os

import requests


DEFAULT_USER_AGENT = "Poke6sMarket/1.0 (+https://www.poke6s.com)"


def build_tcgcsv_session() -> requests.Session:
    """Build a tcgcsv session with an explicit application identity.

    tcgcsv's own examples recommend a clearly identifiable User-Agent. Using a
    shared helper keeps the metadata scripts aligned and makes future header
    adjustments straightforward.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": os.getenv("POKE6S_TCGCSV_USER_AGENT", DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT,
            "Accept": "application/json",
        }
    )
    return session
