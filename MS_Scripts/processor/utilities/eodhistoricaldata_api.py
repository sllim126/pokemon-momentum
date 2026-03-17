'''
Utilities for fetching data from eodhd.com

Classes:
	EodApi
'''

import os
import sys
import socket
from datetime import date, datetime
import time
import logging
from threading import Lock
from typing import Literal, Mapping
import urllib.error
import requests
import pandas as pd
from tenacity import (
	retry,
	retry_if_exception,
	wait,
	before,
	after,
	stop,
)

logger = logging.getLogger(__name__)

EOD_API_TOKEN_ENV = '5cd1e9ba525fc4.84982047'


class PaymentRequiredError(RuntimeError):
	"""Raised when the vendor reports that additional payment is required."""


_RATE_LIMIT_STATUS_CODES: tuple[int, ...] = (429,)
_RATE_LIMIT_HEADER_KEYS: tuple[str, ...] = (
	'x-ratelimit-remaining',
	'x-rate-limit-remaining',
	'ratelimit-remaining',
)


def _should_retry_exception(exc: BaseException) -> bool:
	"""Return True only when an API call failed because of rate limiting."""
	if isinstance(exc, PaymentRequiredError):
		return False

	status_code: int | None = None
	headers: Mapping[str, str] | None = None

	if isinstance(exc, urllib.error.HTTPError):
		status_code = getattr(exc, 'code', None)
		headers = exc.headers or exc.hdrs # type: ignore[attr-defined]
	elif isinstance(exc, urllib.error.URLError):
		reason = getattr(exc, 'reason', None)
		if isinstance(reason, socket.gaierror):
			return reason.errno in {-3}
	elif isinstance(exc, requests.exceptions.HTTPError):
		status_code = exc.response.status_code if exc.response is not None else None
		headers = exc.response.headers if exc.response is not None else None
	else:
		return False

	if status_code in _RATE_LIMIT_STATUS_CODES:
		return True

	if headers:
		for key in _RATE_LIMIT_HEADER_KEYS:
			value = headers.get(key)
			if value is None:
				continue
			try:
				if int(value) <= 0:
					return True
			except (TypeError, ValueError):
				continue
		retry_after_header = headers.get('retry-after') if hasattr(headers, 'get') else None
		if retry_after_header is not None:
			return True

	return False


def _retry_policy():
	"""Return a Tenacity retry predicate that only retries rate limit errors."""
	return retry_if_exception(_should_retry_exception)


class EodApi:
	'''
	Utility class for fetching data from eodhd.com

	Attributes
	----------
	token: str
		eodhistoricaldata access token
	market: str
		market exchange to fetch data from
	api_url: str
		base api url to use for network requests

	Methods
	-------
	get_market_details() -> pd.Series | None
	code_eod(code: str, to_date: date) -> pd.DataFrame | None
	code_div(code: str, to_date: date) -> pd.DataFrame | None
	code_splits(code: str, to_date: date) -> pd.DataFrame | None
	bulk_eod(fetch_date: date, extended: bool) -> pd.DataFrame | None
	bulk_div(fetch_date: date) -> pd.DataFrame | None
	bulk_splits(fetch_date: date) -> pd.DataFrame | None
	code_live(code: str) -> pd.Series | None
	currency_live(currency: str) -> pd.Series | None
	code_intraday(code: str, interval: str, from_date: date, to_date: date) -> pd.DataFrame | None
	code_fundamentals(code: str) -> pd.Series | None
	'''



	def __init__(self, market: str, token: str | None = None, debug: bool = False) -> None :
		token_value = token or os.getenv(EOD_API_TOKEN_ENV)
		if not token_value:
			raise ValueError('EOD API token is required; set EOD_API_TOKEN or pass token=...')
		self.token = token_value
		self.api_url = 'https://eodhd.com/api/'
		self.market_code = market
		self.debug = debug
		self._rate_limit_limit: int | None = None
		self._rate_limit_remaining: int | None = None
		self._rate_limit_lock = Lock()
		self._payment_required_exit_triggered = False


	def get_rate_limit_snapshot(self) -> tuple[int | None, int | None]:
		"""Return the latest known (limit, remaining) tuple from response headers."""
		with self._rate_limit_lock:
			return self._rate_limit_limit, self._rate_limit_remaining


	def _update_rate_limit_from_headers(self, headers: Mapping[str, str | None]):
		"""Persist rate limit info from an HTTP response."""
		limit_header = headers.get('x-ratelimit-limit')
		remaining_header = headers.get('x-ratelimit-remaining')
		with self._rate_limit_lock:
			if limit_header is not None:
				try:
					self._rate_limit_limit = int(limit_header)
				except ValueError:
					pass
			if remaining_header is not None:
				try:
					self._rate_limit_remaining = int(remaining_header)
				except ValueError:
					pass

	def _handle_payment_required(self) -> None:
		"""Raise a single payment-required error when the vendor signals paywall exhaustion."""
		if self._payment_required_exit_triggered:
			return
		self._payment_required_exit_triggered = True
		raise PaymentRequiredError('EOD Historical Data quota exhausted; payment required')

	@staticmethod
	def _coerce_date_column(frame: pd.DataFrame, column: str = 'date') -> pd.DataFrame:
		"""Ensure JSON payload date columns become datetimes for downstream consumers."""
		if not frame.empty and column in frame.columns:
			frame[column] = pd.to_datetime(frame[column], errors='coerce')
		return frame



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def get_market_details(self, from_date: datetime | None = None, to_date: datetime | None = None):
		'''
		Gets the market data for the current market.

			Parameters:
				from_date (datetime | None): If included, will only return holiday data on or after this date
					Defaults to 6 months before the current day
				to_date (datetime | None): If included, will only return holiday data on or before this date
					Defaults to 6 months after the current day

			Returns:
				A pandas Series upon success.
		'''

		market_str = f'{self.api_url}exchange-details/{self.market_code}?api_token={self.token}'
		if from_date is not None:
			market_str = f'{market_str}&from={from_date.strftime("%Y-%m-%d")}'
		if to_date is not None:
			market_str = f'{market_str}&to={to_date.strftime("%Y-%m-%d")}'

		try:
			market_info: pd.Series = pd.read_json(
				market_str,
				orient='records',
				typ='series'
			)
			return market_info
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			logger.exception('Market details URL: %s', market_str, exc_info=e)
			if e.msg == 'Not Found':
				return
			raise


	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def code_eod(
		self,
		code: str,
		to_date: date | None = None) -> pd.DataFrame | None :
		'''
		Downloads the end of data data for a single
		code, optionally up to a specified date.

			Parameters:
				code (str): The fund code to fetch data for.
				to_date (date): Limits the more recent end of the date range.
				Optional, defaults to the current day.

			Returns:
				A pandas DataFrame upon successful download.
		'''

		to_date = to_date or datetime.today().date()
		to_str = to_date.strftime('%Y-%m-%d')
		code = code.replace(' ', '%20')
		logger.debug('Downloading eod data for %s from beginning to %s', code, to_str)

		code_str = f'{self.api_url}eod/{code}.{self.market_code}?api_token={self.token}&to={to_str}&fmt=json'

		try:
			eod: pd.DataFrame = pd.read_json(code_str)
			if eod.shape[0] > 0:
				assert (eod.shape[0] == eod.date.unique().shape[0]), 'Duplicate dates detected in token EOD download'
			return eod
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if e.msg != 'Not Found':
				logger.exception('Code EOD URL: %s', code_str, exc_info=e)
			raise
		except urllib.error.URLError as e:
			logger.warning('Network error fetching EOD data: %s', code_str, exc_info=e)
			return



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def code_div(self, code: str, to_date: date | None = None) -> pd.DataFrame | None :
		'''
		Downloads dividend data for a single
		code, optionally up to a specified date.

			Parameters:
				code (str): The fund code to fetch data for.
				to_date (date): Limits the more recent end of the date range.
				Optional, defaults to the current day.

			Returns:
				A pandas DataFrame upon successful download.
		'''

		append_to_param = to_date is not None
		to_date = to_date or datetime.today().date()
		to_str = to_date.strftime('%Y-%m-%d')
		code = code.replace(' ', '%20')
		logger.debug('Downloading dividend data for %s from beginning to %s', code, to_str)

		code_str = f'{self.api_url}div/{code}.{self.market_code}?api_token={self.token}&fmt=json'
		if append_to_param:
			code_str = f'{code_str}&to={to_str}'

		try:
			div = pd.read_json(code_str)
			if div.shape[0] > 0:
				assert (div.shape[0] == div.date.unique().shape[0]), 'Duplicate dates detected in token dividend download'
			return div
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if e.msg == 'Not Found':
				return
			else:
				logger.exception('Code Div URL: %s', code_str, exc_info=e)
			raise
		except urllib.error.URLError as e:
			logger.warning('Network error fetching dividend data: %s', code_str, exc_info=e)
			return



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def code_splits(self, code: str, to_date: date | None = None) -> pd.DataFrame | None :
		'''
		Downloads splits data for a single
		code, optionally up to a specified date.

			Parameters:
				code (str): The fund code to fetch data for.
				to_date (date): Limits the more recent end of the date range.
				Optional, defaults to the current day.

			Returns:
				A pandas DataFrame upon successful download.
		'''

		to_date = to_date or datetime.today().date()
		to_str = to_date.strftime('%Y-%m-%d')
		code = code.replace(' ', '%20')
		logger.debug(
			'Downloading splits data for %s from beginning to %s', code, to_str
		)

		code_str = f'{self.api_url}splits/{code}.{self.market_code}?api_token={self.token}&to={to_str}&fmt=json'

		try:
			splits = pd.read_json(code_str)
			if splits.shape[0] > 0:
				assert (splits.shape[0] == splits.date.unique().shape[0]), 'Duplicate dates detected in token splits download'
			return splits
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if e.msg == 'Not Found':
				return
			else:
				logger.exception('Code Splits URL: %s', code_str, exc_info=e)
			raise
		except urllib.error.URLError as e:
			logger.warning('Network error fetching splits data: %s', code_str, exc_info=e)
			return


	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def bulk_eod(self, fetch_date: date | None = None, extended: bool = False) -> pd.DataFrame | None :
		'''
		Downloads bulk eod data for the
		entire market for the specified date.

			Parameters:
				fetch_date (date): The date to fetch the bulk data for.
					Optional, defaults to the current day.
				extended (bool): If the extended dataset should be fetched.
					Optional, defaults to False

			Returns:
				A pandas DataFrame upon successful download.
		'''

		fetch_date = fetch_date or datetime.today().date()
		fetch_date_str = fetch_date.strftime('%Y-%m-%d')
		logger.debug('Downloading bulk %s EOD data for %s', self.market_code, fetch_date_str)

		bulk_str = (
			f'{self.api_url}eod-bulk-last-day/{self.market_code}?api_token={self.token}'
			f'&fmt=json&date={fetch_date_str}&filter={"extended" if extended else "0"}'
		)

		try:
			response = requests.get(bulk_str, timeout=60)
			response.raise_for_status()
		except requests.exceptions.HTTPError as e:
			status = e.response.status_code if e.response is not None else None
			if status == 402:
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if status == 404:
				logger.warning('Bulk EOD endpoint returned 404 for %s', fetch_date_str)
				return
			logger.exception('Bulk EOD URL: %s', bulk_str, exc_info=e)
			raise

		self._update_rate_limit_from_headers(response.headers)
		data = response.json()
		eod = self._coerce_date_column(pd.DataFrame(data))
		if eod.shape[0] > 0:
			assert (eod.shape[0] == eod.code.unique().shape[0]), 'Duplicate codes detected in daily EOD download'
		return eod



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def bulk_div(self, fetch_date: date | None = None) -> pd.DataFrame | None :
		'''
		Downloads bulk dividend data for the
		entire market for the specified date.

			Parameters:
				fetch_date (date): The date to fetch the bulk data for.
				Optional, defaults to the current day.

			Returns:
				A pandas DataFrame upon successful download.
		'''

		fetch_date = fetch_date or datetime.today().date()
		fetch_date_str = fetch_date.strftime('%Y-%m-%d')
		logger.debug(
			'Downloading bulk %s dividend data for %s',
			self.market_code, fetch_date_str
		)

		bulk_str = (
			f'{self.api_url}eod-bulk-last-day/{self.market_code}'
			f'?api_token={self.token}&type=dividends&fmt=json&date={fetch_date_str}'
		)

		try:
			response = requests.get(bulk_str, timeout=60)
			response.raise_for_status()
		except requests.exceptions.HTTPError as e:
			status = e.response.status_code if e.response is not None else None
			if status == 402:
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if status == 404:
				logger.warning('Bulk dividend endpoint returned 404 for %s', fetch_date_str)
				return
			logger.exception('Bulk Div URL: %s', bulk_str, exc_info=e)
			raise

		self._update_rate_limit_from_headers(response.headers)
		data = response.json()
		div = self._coerce_date_column(pd.DataFrame(data))
		if div.shape[0] > 0:
			assert (div.shape[0] == div.code.unique().shape[0]), 'Duplicate codes detected in daily div download'
		return div



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def bulk_splits(self, fetch_date: date | None = None) -> pd.DataFrame | None :
		'''
		Downloads bulk splits data for the
		entire market for the specified date.

			Parameters:
				fetch_date (date): The date to fetch the bulk data for.
				Optional, defaults to the current day.

			Returns:
				A pandas DataFrame upon successful download.
		'''

		fetch_date = fetch_date or datetime.today().date()
		fetch_date_str = fetch_date.strftime('%Y-%m-%d')
		logger.debug(
			'Downloading bulk %s splits data for %s',
			self.market_code, fetch_date_str
		)

		bulk_str = (
			f'{self.api_url}eod-bulk-last-day/{self.market_code}'
			f'?api_token={self.token}&type=splits&fmt=json&date={fetch_date_str}'
		)

		try:
			response = requests.get(bulk_str, timeout=60)
			response.raise_for_status()
		except requests.exceptions.HTTPError as e:
			status = e.response.status_code if e.response is not None else None
			if status == 402:
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if status == 404:
				logger.warning('Bulk splits endpoint returned 404 for %s', fetch_date_str)
				return
			logger.exception('Bulk Splits URL: %s', bulk_str, exc_info=e)
			raise

		self._update_rate_limit_from_headers(response.headers)
		data = response.json()
		splits = self._coerce_date_column(pd.DataFrame(data))
		if splits.shape[0] > 0:
			assert (splits.shape[0] == splits.code.unique().shape[0]), 'Duplicate codes detected in daily splits download'
		return splits



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def code_live(self, code: str) -> pd.Series | None :
		'''
		Downloads the real-time (delayed)
		pricing data for the specified code.

			Parameters:
				code (str): The fund code to fetch data for.

			Returns:
				A pandas Series upon successful download.
		'''

		logger.debug('Downloading real-time exchange rate for %s', code)

		code_str = f'{self.api_url}real-time/{code}.{self.market_code}?api_token={self.token}&fmt=json'

		try:
			live_data = pd.read_json(code_str, typ='series')

			if live_data['timestamp'] == 'NA':
				logger.warning('No real-time data available for %s', code)
				return

			return live_data
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			logger.exception('Code Live URL: %s', code_str, exc_info=e)
			if e.msg == 'Not Found':
				return
			raise



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def currency_live(self, currency: str) -> pd.Series | None :
		'''
		Downloads the real-time (delayed) FOREX currency
		exchange information for the specified currency.

			Parameters:
				currency (str): The currency to fetch data for.

			Returns:
				A pandas Series upon successful download.
		'''

		logger.debug('Downloading real-time exchange rate for %s', currency)

		currency_str = f'{self.api_url}real-time/{currency}.FOREX?api_token={self.token}&fmt=json'

		try:
			live_data = pd.read_json(currency_str, typ='series')
			return live_data
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			logger.exception('Currency Live URL: %s', currency_str, exc_info=e)
			if e.msg == 'Not Found':
				return
			raise



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def code_intraday(
		self, code: str, interval: str, from_date: date,
		to_date: date | None = None
	) -> pd.DataFrame | Literal[False] | None :
		'''
		Downloads the intraday historical
		pricing data for the specified code.

			Parameters:
				code (str): The fund code to fetch data for.
				interval (str): The data interval. Must be one of [1m, 5m, 1h].
				from_date (date): The start (older) of the data range.
				to_date (date): The end (newer) of the data range. Optional, defaults to the current day.

			Returns:
				A pandas DataFrame upon successful download.
		'''

		if interval not in ['1m', '5m', '1h']:
			logger.error(
				'Invalid interval specified: %s. Expecting one of [1m, 5m, 1h]',
				interval
			)
			return False

		to_date = to_date or datetime.today().date()
		from_str = from_date.strftime('%c')
		to_str = to_date.strftime('%c')

		from_ts = time.mktime(from_date.timetuple())
		to_ts = time.mktime(to_date.timetuple())

		logger.debug(
			'Downloading intraday data for %s from %s (%s) to %s (%s) with interval %s',
			code, from_str, from_ts, to_str, to_ts, interval
		)

		code_str = (
			f'{self.api_url}intraday/{code}.{self.market_code}?api_token={self.token}'
			f'&interval={interval}&from={from_ts}&to={to_ts}&fmt=json'
		)

		try:
			intraday_data = pd.read_json(code_str)
			return intraday_data
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			logger.exception('Code Intraday URL: %s', code_str, exc_info=e)
			if e.msg == 'Not Found':
				return
			raise



	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def code_fundamentals(self, code: str) -> pd.Series | None :
		'''
		Downloads fundamentals information for the specified code.

			Parameters:
				code (str): The fund code to fetch data for.

			Returns:
				A pandas Series upon successful download.
		'''

		logger.debug('Downloading fundamentals data for %s', code)

		code_str = (
			f'{self.api_url}fundamentals/{code}.{self.market_code}?api_token={self.token}&fmt=json'
		)

		try:
			fundamentals = pd.read_json(code_str, typ='series')
			return fundamentals
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			if e.msg not in ['Not Found', 'Unprocessable Content']:
				logger.exception('Code Fundamentals URL: %s', code_str, exc_info=e)
			raise


	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def get_index_list(self) -> pd.Series:
		'''
		Gets the list of available indices.

			Returns:
				A pandas Series upon success.
		'''

		indices_str = (
			'https://eodhd.com/financial-apis/wp-content/uploads/2024/10/'
			'EODHD-Fundamentals_-Available-Index-list-with-component-details.xlsx'
		)

		try:
			indices = pd.read_excel(indices_str)
			return indices.iloc[:, 0]
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			logger.exception('Indices URL: %s', indices_str, exc_info=e)
			raise


	@retry(
		wait=wait.wait_random_exponential(multiplier=1, max=60),
		stop=stop.stop_after_attempt(10),
		reraise=True,
		retry=_retry_policy(),
		before=before.before_log(logger, logging.DEBUG),
		after=after.after_log(logger, logging.DEBUG))
	def get_index_components(self, index: str) -> tuple[pd.DataFrame, pd.DataFrame]:
		'''
		Gets the index details and the list of components for the specified index.

			Parameters:
				index (str): The index to fetch components for.

			Returns:
				A tuple of pandas DataFrames upon success, where the first
				DataFrame contains the index details and the second
				DataFrame contains the list of components.
		'''

		index_str = f'https://eodhd.com/api/fundamentals/{index}.INDX?api_token={self.token}&fmt=json'

		try:
			index_data = pd.read_json(index_str, typ='series')
			general = pd.DataFrame.from_dict([index_data.get('General', {})]) # type: ignore
			components = pd.DataFrame.from_dict(index_data.get('Components', {}), orient='index') # type: ignore
			return general, components
		except urllib.error.HTTPError as e:
			if e.msg == 'Payment Required':
				logger.exception('Out of API calls', exc_info=e)
				self._handle_payment_required()
			logger.exception('Index Components URL: %s', index_str, exc_info=e)
			raise
