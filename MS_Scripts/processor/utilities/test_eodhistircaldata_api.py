
'''
Tests for eodhistoricaldata_api.

The majority of this module should be validated via mocks/stubs to keep CI deterministic.
Small live guardrail checks can be opted-into via an env var to watch for upstream schema changes.
'''
import datetime
from http.client import HTTPMessage
import os
import unittest
import urllib.error
from typing import Callable, cast
from unittest import TestCase, mock

import pandas as pd
from pandas.testing import assert_index_equal
import requests

from processor.utilities.eodhistoricaldata_api import (
	EodApi,
	PaymentRequiredError,
	logger,
	_should_retry_exception,
)


LIVE_GUARDRAIL_ENV = 'RUN_EOD_LIVE_GUARDRAILS'
RUN_LIVE_GUARDRAILS = os.getenv(LIVE_GUARDRAIL_ENV) == '1'


def _http_error(msg: str = 'Not Found', code: int = 404) -> urllib.error.HTTPError:
	"""Helper that mirrors the HTTPError raised by urllib."""
	return urllib.error.HTTPError(
		url='https://example.com',
		code=code,
		msg=msg,
		hdrs=None, # type: ignore[arg-type]
		fp=None
	)


def _http_error_with_headers(code: int, msg: str, headers: dict[str, str]) -> urllib.error.HTTPError:
	message = HTTPMessage()
	for key, value in headers.items():
		message[key] = value
	return urllib.error.HTTPError(
		url='https://example.com',
		code=code,
		msg=msg,
		hdrs=message,
		fp=None
	)


class TestEodApiUnit(TestCase):
	'''Mock-heavy unit tests that cover edge-cases without hitting the network.'''

	def setUp(self):
		super().setUp()
		self.api = EodApi('US', token='test-token', debug=True)

	def test_init_without_token_raises_value_error(self):
		with mock.patch.dict(os.environ, {}, clear=True):
			with self.assertRaises(ValueError):
				EodApi('US')

	def test_handle_payment_required_exits_once(self):
		with self.assertRaises(PaymentRequiredError):
			self.api._handle_payment_required()
		self.api._handle_payment_required()

	def test_should_retry_exception_payment_required_returns_false(self):
		self.assertFalse(_should_retry_exception(PaymentRequiredError('quota')))

	def test_should_retry_exception_urllib_rate_limit_code(self):
		err = _http_error_with_headers(429, 'Too Many Requests', {'x-ratelimit-remaining': '10'})
		self.assertTrue(_should_retry_exception(err))

	def test_should_retry_exception_urllib_zero_remaining_header(self):
		err = _http_error_with_headers(500, 'Server Error', {'x-ratelimit-remaining': '0'})
		self.assertTrue(_should_retry_exception(err))

	def test_should_retry_exception_urllib_retry_after_header(self):
		err = _http_error_with_headers(500, 'Server Error', {'retry-after': '9'})
		self.assertTrue(_should_retry_exception(err))

	def test_should_retry_exception_urllib_invalid_remaining_header(self):
		err = _http_error_with_headers(500, 'Server Error', {'x-ratelimit-remaining': 'oops'})
		self.assertFalse(_should_retry_exception(err))

	def test_should_retry_exception_requests_retry_after_header(self):
		response = mock.Mock()
		response.status_code = 500
		response.headers = {'retry-after': '5'}
		exc = requests.exceptions.HTTPError(response=response)
		self.assertTrue(_should_retry_exception(exc))

	def test_should_retry_exception_non_rate_limit_http_error_returns_false(self):
		response = mock.Mock(status_code=500, headers={})
		exc = requests.exceptions.HTTPError(response=response)
		self.assertFalse(_should_retry_exception(exc))

	def test_rate_limit_snapshot_updates(self):
		self.api._update_rate_limit_from_headers({
			'x-ratelimit-limit': '200',
			'x-ratelimit-remaining': '180'
		})
		self.assertEqual(self.api.get_rate_limit_snapshot(), (200, 180))

		self.api._update_rate_limit_from_headers({
			'x-ratelimit-limit': 'bad-value',
			'x-ratelimit-remaining': '90'
		})
		self.assertEqual(self.api.get_rate_limit_snapshot(), (200, 90))

		self.api._update_rate_limit_from_headers({
			'x-ratelimit-limit': '300',
			'x-ratelimit-remaining': 'oops'
		})
		self.assertEqual(self.api.get_rate_limit_snapshot(), (300, 90))

	def test_get_market_details_builds_expected_url(self):
		mock_series = pd.Series({'Code': 'US'})
		start = datetime.datetime(2024, 1, 1)
		end = datetime.datetime(2024, 6, 1)
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=mock_series) as mock_read:
			res = self.api.get_market_details(start, end)

		expected_url = (
			f'{self.api.api_url}exchange-details/{self.api.market_code}?api_token={self.api.token}'
			'&from=2024-01-01&to=2024-06-01'
		)
		mock_read.assert_called_once_with(expected_url, orient='records', typ='series')
		self.assertIs(res, mock_series)

	def test_get_market_details_not_found_returns_none(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			self.assertIsNone(self.api.get_market_details())

	def test_get_market_details_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.get_market_details.__wrapped__(self.api)
		mock_handle.assert_called_once_with()

	def test_code_eod_success_and_code_encoding(self):
		df = pd.DataFrame({
			'date': pd.to_datetime(['2024-01-02', '2024-01-03']),
			'open': [1, 2],
			'high': [2, 3],
			'low': [0.5, 1.5],
			'close': [1.5, 2.5],
			'adjusted_close': [1.4, 2.4],
			'volume': [100, 200]
		})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=df) as mock_read:
			res = self.api.code_eod('ABC DEF', datetime.date(2024, 1, 3))
		self.assertIsNotNone(res)
		if res is not None:
			expected_url = (
				f'{self.api.api_url}eod/ABC%20DEF.{self.api.market_code}?'
				f'api_token={self.api.token}&to=2024-01-03&fmt=json'
			)
			mock_read.assert_called_once_with(expected_url)
			self.assertTrue(res.equals(df))

	def test_code_eod_duplicate_dates_raise_assertion(self):
		df = pd.DataFrame({
			'date': pd.to_datetime(['2024-01-02', '2024-01-02']),
			'open': [1, 2],
			'high': [2, 3],
			'low': [0.5, 1.5],
			'close': [1.5, 2.5],
			'adjusted_close': [1.4, 2.4],
			'volume': [100, 200]
		})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=df):
			with self.assertRaises(AssertionError):
				self.api.code_eod('AAPL')

	def test_code_eod_payment_required_exits(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required', side_effect=PaymentRequiredError('mock')) as mock_handle:
				with self.assertRaises(PaymentRequiredError):
					EodApi.code_eod.__wrapped__(self.api, 'AAPL')
		mock_handle.assert_called_once_with()

	def test_code_eod_other_error_logs_and_raises(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Server Error', 500)):
			with self.assertRaises(urllib.error.HTTPError):
				EodApi.code_eod.__wrapped__(self.api, 'AAPL')

	def test_code_div_appends_to_param_for_custom_date(self):
		df = pd.DataFrame({
			'date': pd.to_datetime(['2024-01-05']),
			'declarationDate': pd.to_datetime(['2024-01-01']),
			'recordDate': pd.to_datetime(['2024-01-02']),
			'paymentDate': pd.to_datetime(['2024-01-06']),
			'period': ['Q1'],
			'value': [1.0],
			'unadjustedValue': [1.0],
			'currency': ['USD']
		})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=df) as mock_read:
			self.api.code_div('AAPL', datetime.date(2024, 1, 5))

		expected_url = (
			f'{self.api.api_url}div/AAPL.{self.api.market_code}?api_token={self.api.token}&fmt=json&to=2024-01-05'
		)
		mock_read.assert_called_once_with(expected_url)

	def test_code_div_not_found_returns_none(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			self.assertIsNone(self.api.code_div('MISSING'))

	def test_code_div_payment_required_calls_handler_and_raises(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.code_div.__wrapped__(self.api, 'AAPL')
		mock_handle.assert_called_once_with()

	def test_code_splits_not_found_returns_none(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			self.assertIsNone(self.api.code_splits('MISSING'))

	def test_code_splits_other_error_bubbles(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Server Error', 500)):
			with self.assertRaises(urllib.error.HTTPError):
				self.api.code_splits('ERR')

	def test_code_splits_success_enforces_unique_dates(self):
		df = pd.DataFrame({'date': pd.to_datetime(['2024-01-02']), 'ratio': ['2:1']})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=df):
			res = self.api.code_splits('AAPL')
		self.assertIsNotNone(res)

	def test_code_splits_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.code_splits.__wrapped__(self.api, 'AAPL')
		mock_handle.assert_called_once_with()

	def test_bulk_eod_success_updates_rate_limit(self):
		payload = [{'code': 'AAPL', 'exchange_short_name': 'US', 'date': '2024-01-02', 'open': 1,
			'high': 2, 'low': 0.5, 'close': 1.5, 'adjusted_close': 1.5, 'volume': 10}]
		mock_response = mock.Mock()
		mock_response.raise_for_status.return_value = None
		mock_response.json.return_value = payload
		mock_response.headers = {
			'x-ratelimit-limit': '500',
			'x-ratelimit-remaining': '499'
		}
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response) as mock_get:
			res = self.api.bulk_eod(datetime.date(2024, 1, 2), extended=False)
		self.assertIsNotNone(res)
		if res is not None:
			expected_url = (
				f'{self.api.api_url}eod-bulk-last-day/{self.api.market_code}?api_token={self.api.token}'
				'&fmt=json&date=2024-01-02&filter=0'
			)
			mock_get.assert_called_once_with(expected_url, timeout=60)
			self.assertEqual(self.api.get_rate_limit_snapshot(), (500, 499))
			self.assertEqual(res.shape[0], 1)

	def test_bulk_eod_handles_404(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=404))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			self.assertIsNone(self.api.bulk_eod(datetime.date(2024, 1, 2)))

	def test_bulk_eod_payment_required_exits(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=402))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			with mock.patch.object(self.api, '_handle_payment_required', side_effect=PaymentRequiredError('mock')) as mock_handle:
				with self.assertRaises(PaymentRequiredError):
					EodApi.bulk_eod.__wrapped__(self.api, datetime.date(2024, 1, 2))
		mock_handle.assert_called_once_with()

	def test_bulk_eod_other_http_errors_raise(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=500))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			with self.assertRaises(requests.exceptions.HTTPError):
				EodApi.bulk_eod.__wrapped__(self.api, datetime.date(2024, 1, 2))

	def test_bulk_div_success(self):
		payload = [{'code': 'AAPL', 'exchange': 'US', 'date': '2024-01-02', 'dividend': 0.1,
			'currency': 'USD', 'declarationDate': '2023-12-10', 'recordDate': '2023-12-15',
			'paymentDate': '2024-01-02', 'period': 'Q', 'unadjustedValue': 0.1}]
		mock_response = mock.Mock()
		mock_response.raise_for_status.return_value = None
		mock_response.json.return_value = payload
		mock_response.headers = {}
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			res = self.api.bulk_div(datetime.date(2024, 1, 2))
		self.assertIsNotNone(res)
		if res is not None:
			self.assertEqual(res.shape[0], 1)

	def test_bulk_div_payment_required_calls_handler(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=402))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(requests.exceptions.HTTPError):
					EodApi.bulk_div.__wrapped__(self.api, datetime.date(2024, 1, 2))
		mock_handle.assert_called_once_with()

	def test_bulk_div_handles_404(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=404))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			self.assertIsNone(self.api.bulk_div(datetime.date(2024, 1, 2)))

	def test_bulk_div_other_http_errors_raise(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=500))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			with self.assertRaises(requests.exceptions.HTTPError):
				EodApi.bulk_div.__wrapped__(self.api, datetime.date(2024, 1, 2))

	def test_bulk_splits_success(self):
		payload = [{'code': 'AAPL', 'exchange': 'US', 'date': '2024-01-02', 'split': '2:1'}]
		mock_response = mock.Mock()
		mock_response.raise_for_status.return_value = None
		mock_response.json.return_value = payload
		mock_response.headers = {}
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			res = self.api.bulk_splits(datetime.date(2024, 1, 2))
		self.assertIsNotNone(res)
		if res is not None:
			self.assertEqual(res.shape[0], 1)

	def test_bulk_splits_payment_required_calls_handler(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=402))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(requests.exceptions.HTTPError):
					EodApi.bulk_splits.__wrapped__(self.api, datetime.date(2024, 1, 2))
		mock_handle.assert_called_once_with()

	def test_bulk_splits_other_http_errors_raise(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=500))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			with self.assertRaises(requests.exceptions.HTTPError):
				EodApi.bulk_splits.__wrapped__(self.api, datetime.date(2024, 1, 2))

	def test_bulk_splits_handles_404(self):
		http_exc = requests.exceptions.HTTPError(response=mock.Mock(status_code=404))
		mock_response = mock.Mock()
		mock_response.raise_for_status.side_effect = http_exc
		with mock.patch('processor.utilities.eodhistoricaldata_api.requests.get', return_value=mock_response):
			self.assertIsNone(self.api.bulk_splits(datetime.date(2024, 1, 2)))

	def test_code_live_timestamp_na_returns_none(self):
		series = pd.Series({'timestamp': 'NA', 'code': 'AAPL.US'})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=series):
			self.assertIsNone(self.api.code_live('AAPL'))

	def test_code_live_not_found_returns_none(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			self.assertIsNone(self.api.code_live('MISSING'))

	def test_code_live_success_returns_series(self):
		series = pd.Series({'timestamp': '123', 'code': 'AAPL.US'})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=series):
			res = self.api.code_live('AAPL')
		self.assertIs(res, series)

	def test_code_live_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.code_live.__wrapped__(self.api, 'AAPL')
		mock_handle.assert_called_once_with()

	def test_currency_live_not_found_returns_none(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			self.assertIsNone(self.api.currency_live('ZZZ'))

	def test_currency_live_success_returns_series(self):
		series = pd.Series({'timestamp': '123', 'code': 'USD'})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=series):
			res = self.api.currency_live('USD')
		self.assertIs(res, series)

	def test_currency_live_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.currency_live.__wrapped__(self.api, 'USD')
		mock_handle.assert_called_once_with()

	def test_code_intraday_invalid_interval_logs_once(self):
		with mock.patch.object(logger, 'error') as mock_error:
			self.assertFalse(self.api.code_intraday('AAPL', '1d', datetime.date(2024, 1, 2)))
		mock_error.assert_called_once()

	def test_code_intraday_success(self):
		df = pd.DataFrame({'timestamp': pd.to_datetime(['2024-01-02']), 'gmtoffset': [0], 'datetime': [''],
			'open': [1], 'high': [2], 'low': [0.5], 'close': [1.5], 'volume': [10]})
		with mock.patch('processor.utilities.eodhistoricaldata_api.time.mktime', side_effect=[1000.0, 2000.0]):
			with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=df) as mock_read:
				res = self.api.code_intraday('AAPL', '1m', datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
		self.assertIsNotNone(res)
		self.assertIsNot(res, False)
		if isinstance(res, pd.DataFrame):
			expected_url = (
				f'{self.api.api_url}intraday/AAPL.{self.api.market_code}?api_token={self.api.token}'
				'&interval=1m&from=1000.0&to=2000.0&fmt=json'
			)
			mock_read.assert_called_once_with(expected_url)
			self.assertTrue(res.equals(df))

	def test_code_intraday_not_found_returns_none(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			self.assertIsNone(self.api.code_intraday('MISSING', '1m', datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)))

	def test_code_intraday_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.code_intraday.__wrapped__(self.api, 'AAPL', '1m', datetime.date(2024, 1, 1), datetime.date(2024, 1, 2))
		mock_handle.assert_called_once_with()

	def test_code_fundamentals_success(self):
		series = pd.Series({'General': {'Code': 'AAPL'}, 'Highlights': {}, 'Valuation': {}, 'SharesStats': {},
			'Technicals': {}, 'SplitsDividends': {}, 'AnalystRatings': {}, 'Holders': {},
			'InsiderTransactions': {}, 'ESGScores': {}, 'outstandingShares': {}, 'Earnings': {}, 'Financials': {}})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=series):
			res = self.api.code_fundamentals('AAPL')
		self.assertIs(res, series)

	def test_code_fundamentals_not_found_raises(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			with self.assertRaises(urllib.error.HTTPError):
				self.api.code_fundamentals('MISSING')

	def test_code_fundamentals_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					EodApi.code_fundamentals.__wrapped__(self.api, 'AAPL')
		mock_handle.assert_called_once_with()

	def test_code_fundamentals_other_errors_raise(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Server Error', 500)):
			with self.assertRaises(urllib.error.HTTPError):
				EodApi.code_fundamentals.__wrapped__(self.api, 'AAPL')

	def test_get_index_list_returns_first_column(self):
		frame = pd.DataFrame({
			'index': ['DJI.INDX', 'SPX.INDX'],
			'extra': [1, 2]
		})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_excel', return_value=frame):
			series = self.api.get_index_list()
		self.assertListEqual(series.tolist(), ['DJI.INDX', 'SPX.INDX'])

	def test_get_index_list_payment_required_calls_exit(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_excel', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required', side_effect=PaymentRequiredError('mock')) as mock_handle:
				with self.assertRaises(PaymentRequiredError):
					self.api.get_index_list()
		mock_handle.assert_called_once_with()

	def test_get_index_list_other_http_error_bubbles(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_excel', side_effect=_http_error('Server Error', 500)):
			with self.assertRaises(urllib.error.HTTPError):
				self.api.get_index_list()

	def test_get_index_components_success(self):
		series = pd.Series({
			'General': {'Name': 'Dow Jones'},
			'Components': {
				'AAPL': {'Code': 'AAPL'},
				'MSFT': {'Code': 'MSFT'}
			}
		})
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', return_value=series):
			general, components = self.api.get_index_components('DJI')
		self.assertEqual(general.iloc[0]['Name'], 'Dow Jones')
		self.assertIn('AAPL', components.index)

	def test_get_index_components_error_bubbles(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error()):
			with self.assertRaises(urllib.error.HTTPError):
				self.api.get_index_components('BAD')

	def test_get_index_components_payment_required_calls_handler(self):
		with mock.patch('processor.utilities.eodhistoricaldata_api.pd.read_json', side_effect=_http_error('Payment Required', 402)):
			with mock.patch.object(self.api, '_handle_payment_required') as mock_handle:
				with self.assertRaises(urllib.error.HTTPError):
					self.api.get_index_components('DJI')
		mock_handle.assert_called_once_with()


@unittest.skipUnless(RUN_LIVE_GUARDRAILS, f'Set {LIVE_GUARDRAIL_ENV}=1 to run live API guardrail tests')
class TestEodApiLiveGuardrails(TestCase):
	'''Opt-in live tests that validate schemas stay stable when upstream changes occur.'''

	BULK_DIV_DATES = [
		datetime.date(2024, 11, 1),
		datetime.date(2024, 8, 1),
		datetime.date(2023, 12, 15),
		datetime.date(2020, 9, 1),
	]
	BULK_SPLITS_DATES = [
		datetime.date(2024, 11, 1),
		datetime.date(2024, 5, 1),
		datetime.date(2023, 1, 3),
		datetime.date(2020, 9, 1),
	]

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		token = os.getenv('EOD_API_TOKEN')
		if not token:
			raise unittest.SkipTest('EOD_API_TOKEN must be set to run live guardrails')
		cls.api = EodApi('US', token=token, debug=True)

	def _fetch_or_fail(
		self,
		func: Callable[..., pd.DataFrame | pd.Series | None],
		*args,
		**kwargs,
	) -> pd.DataFrame | pd.Series:
		try:
			result = func(*args, **kwargs)
		except (urllib.error.HTTPError, requests.exceptions.HTTPError) as exc:
			self.fail(f'Live guardrail call failed: {exc}')
		self.assertIsNotNone(result)
		return cast(pd.DataFrame | pd.Series, result)

	def _fetch_bulk_frame_or_skip(
		self,
		func: Callable[[datetime.date | None], pd.DataFrame | None],
		candidate_dates: list[datetime.date],
		**kwargs,
	) -> pd.DataFrame:
		for fetch_date in candidate_dates:
			try:
				result = func(fetch_date, **kwargs)
			except (urllib.error.HTTPError, requests.exceptions.HTTPError) as exc:
				self.fail(f'Live guardrail call failed: {exc}')
			if result is None or result.empty:
				continue
			return result
		self.skipTest(
			f'No data returned for {func.__name__} using candidate dates {candidate_dates}; '
			'Set different RUN_EOD_LIVE_GUARDRAILS parameters if needed.'
		)

	def test_live_code_eod_schema(self):
		res = self._fetch_or_fail(self.api.code_eod, 'AAPL')
		assert_index_equal(res.columns, pd.Index([
			'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume'
		]))

	def test_live_bulk_eod_schema(self):
		res = self._fetch_or_fail(self.api.bulk_eod, datetime.date(2020, 1, 2))
		self.assertGreater(res.shape[0], 0)
		assert_index_equal(res.columns, pd.Index([
			'code', 'exchange_short_name', 'date', 'open', 'high', 'low', 'close',
			'adjusted_close', 'volume'
		]))

	def test_live_code_live_fields(self):
		series = self._fetch_or_fail(self.api.code_live, 'AAPL')
		assert_index_equal(series.index, pd.Index([
			'code', 'timestamp', 'gmtoffset', 'open', 'high', 'low', 'close',
			'volume', 'previousClose', 'change', 'change_p'
		]))
		self.assertTrue(series.timestamp != 'NA')

	def test_live_code_fundamentals_sections(self):
		series = self._fetch_or_fail(self.api.code_fundamentals, 'AAPL')
		for section in ['General', 'Highlights', 'Valuation', 'SharesStats', 'Technicals', 'Financials']:
			self.assertIn(section, series.index)

	def test_live_get_market_details_fields(self):
		series = self._fetch_or_fail(self.api.get_market_details)
		self.assertIn('Code', series.index)
		self.assertEqual(series.get('Code'), self.api.market_code)

	def test_live_code_div_schema(self):
		frame = self._fetch_or_fail(self.api.code_div, 'AAPL')
		self.assertTrue({'date', 'currency', 'value'}.issubset(set(frame.columns)))

	def test_live_code_splits_schema(self):
		frame = self._fetch_or_fail(self.api.code_splits, 'AAPL', datetime.date(2024, 1, 1))
		self.assertTrue({'date', 'ratio'}.issubset(set(frame.columns)))

	def test_live_bulk_div_schema(self):
		frame = self._fetch_bulk_frame_or_skip(self.api.bulk_div, self.BULK_DIV_DATES)
		self.assertTrue({'code', 'date', 'dividend'}.issubset(set(frame.columns)))

	def test_live_bulk_splits_schema(self):
		frame = self._fetch_bulk_frame_or_skip(self.api.bulk_splits, self.BULK_SPLITS_DATES)
		self.assertTrue({'code', 'date', 'split'}.issubset(set(frame.columns)))

	def test_live_currency_live_fields(self):
		series = self._fetch_or_fail(self.api.currency_live, 'EURUSD')
		self.assertIn('timestamp', series.index)
		self.assertIn('close', series.index)

	def test_live_code_intraday_schema(self):
		end = datetime.date.today() - datetime.timedelta(days=1)
		while end.weekday() >= 5:
			end -= datetime.timedelta(days=1)
		start = end - datetime.timedelta(days=1)
		try:
			frame = self.api.code_intraday('AAPL', '1h', start, end)
		except (urllib.error.HTTPError, requests.exceptions.HTTPError) as exc:
			self.fail(f'Live guardrail call failed: {exc}')
		if frame in (None, False):
			self.skipTest('No intraday data returned for the selected guardrail window')
		self.assertIsInstance(frame, pd.DataFrame)
		self.assertTrue({'timestamp', 'close', 'volume'}.issubset(set(frame.columns)))

	def test_live_get_index_list_not_empty(self):
		series = self._fetch_or_fail(self.api.get_index_list)
		self.assertGreater(len(series), 0)

	def test_live_get_index_components_structure(self):
		try:
			general, components = self.api.get_index_components('DJI')
		except (urllib.error.HTTPError, requests.exceptions.HTTPError) as exc:
			self.fail(f'Live guardrail call failed: {exc}')
		self.assertFalse(general.empty)
		self.assertIn('Name', general.columns)
		self.assertFalse(components.empty)
		self.assertIn('AAPL', components.index)

