import unittest
from unittest import mock

from scripts.utilities import update_market_dns as dns


class ParseIpTests(unittest.TestCase):
    """The public IP is read from Cloudflare's trace page or a bare answer, IPv4 only."""

    def test_trace_answer(self):
        trace = "fl=123\nh=1.1.1.1\nip=9.9.9.9\nts=1\n"
        self.assertEqual(dns.parse_ip(trace), "9.9.9.9")

    def test_bare_answer(self):
        self.assertEqual(dns.parse_ip("8.8.4.4\n"), "8.8.4.4")

    def test_rejects_private_ipv6_and_junk(self):
        self.assertIsNone(dns.parse_ip("ip=192.168.1.30"))
        self.assertIsNone(dns.parse_ip("ip=2001:4860:4860::8888"))
        self.assertIsNone(dns.parse_ip("<html>error</html>"))


class PlanTests(unittest.TestCase):
    """Only records pointing elsewhere are changed."""

    def test_only_stale_records(self):
        records = [
            {"id": "a", "name": "market.poke6s.com", "content": "1.1.1.1"},
            {"id": "b", "name": "other.poke6s.com", "content": "8.8.8.8"},
        ]
        changes = dns.plan_changes(records, "8.8.8.8")
        self.assertEqual([c["id"] for c in changes], ["a"])
        self.assertEqual(changes[0]["old"], "1.1.1.1")


class MainTests(unittest.TestCase):
    """The run patches only the IP of a stale record, and fails loudly without a token."""

    def settings(self, values):
        return lambda name, default="": values.get(name, default)

    def test_missing_token_fails(self):
        with mock.patch.object(dns, "setting", self.settings({})), \
             mock.patch("sys.argv", ["x"]):
            self.assertEqual(dns.main(), 1)

    def test_updates_stale_record_with_ip_only(self):
        calls = []

        def fake_api(token, method, path, body=None):
            calls.append((method, path, body))
            if path.startswith("/zones?"):
                return [{"id": "Z"}]
            if method == "GET":
                return [{"id": "R", "name": "market.poke6s.com", "content": "1.1.1.1", "proxied": False}]
            return {}

        with mock.patch.object(dns, "setting", self.settings({"CLOUDFLARE_DNS_API_TOKEN": "t"})), \
             mock.patch.object(dns, "public_ip", return_value="8.8.8.8"), \
             mock.patch.object(dns, "cloudflare", side_effect=fake_api), \
             mock.patch.object(dns, "notify") as notify, \
             mock.patch("sys.argv", ["x"]):
            self.assertEqual(dns.main(), 0)
        self.assertIn(("PATCH", "/zones/Z/dns_records/R", {"content": "8.8.8.8"}), calls)
        notify.assert_called_once()

    def test_dry_run_changes_nothing(self):
        def fake_api(token, method, path, body=None):
            self.assertNotEqual(method, "PATCH")
            if path.startswith("/zones?"):
                return [{"id": "Z"}]
            return [{"id": "R", "name": "market.poke6s.com", "content": "1.1.1.1"}]

        with mock.patch.object(dns, "setting", self.settings({"CLOUDFLARE_DNS_API_TOKEN": "t"})), \
             mock.patch.object(dns, "public_ip", return_value="8.8.8.8"), \
             mock.patch.object(dns, "cloudflare", side_effect=fake_api), \
             mock.patch.object(dns, "notify") as notify, \
             mock.patch("sys.argv", ["x", "--dry-run"]):
            self.assertEqual(dns.main(), 0)
        notify.assert_not_called()

    def test_api_error_fails(self):
        with mock.patch.object(dns, "setting", self.settings({"CLOUDFLARE_DNS_API_TOKEN": "t"})), \
             mock.patch.object(dns, "public_ip", return_value="8.8.8.8"), \
             mock.patch.object(dns, "cloudflare", side_effect=dns.DnsError("HTTP 403")), \
             mock.patch("sys.argv", ["x"]):
            self.assertEqual(dns.main(), 1)


if __name__ == "__main__":
    unittest.main()
