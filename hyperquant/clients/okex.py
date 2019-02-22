import time

from urllib.parse import urljoin, urlencode
from hyperquant.api import ParamName, Endpoint, Sorting, Direction, ErrorCode, Platform
from hyperquant.clients import PrivatePlatformRESTClient, Trade, Error, RESTConverter


class OkexRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://www.okex.com/api/v{version}/"

    IS_SORTING_ENABLED = False

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.TRADE: "trades/{symbol}",
        Endpoint.TRADE_HISTORY: "trades/{symbol}",  # same, not implemented for this version
    }
    param_name_lookup = {
        ParamName.LIMIT: "limit_trades",
        ParamName.IS_USE_MAX_LIMIT: None,
        ParamName.SORTING: None,  # not supported
        ParamName.FROM_ITEM: "timestamp",
        ParamName.TO_ITEM: "timestamp",  # ?
        ParamName.FROM_TIME: "timestamp",
        ParamName.TO_TIME: None,  # ?
    }
    param_value_lookup = {
        # Sorting.ASCENDING: None,
        # Sorting.DESCENDING: None,
        Sorting.DEFAULT_SORTING: Sorting.DESCENDING,
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,
        Endpoint.TRADE_HISTORY: 1000,  # same, not implemented for this version
    }

    # For parsing

    param_lookup_by_class = {
        Error: {
            "message": "code",
        },
        Trade: {
            "tid": ParamName.ITEM_ID,
            "timestamp": ParamName.TIMESTAMP,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "type": ParamName.DIRECTION,
        },
    }

    error_code_by_platform_error_code = {
        # "": ErrorCode.UNAUTHORIZED,
        "Unknown symbol": ErrorCode.WRONG_SYMBOL,
        # "ERR_RATE_LIMIT": ErrorCode.RATE_LIMIT,
    }
    error_code_by_http_status = {
        429: ErrorCode.RATE_LIMIT,
    }

    # For converting time
    # is_source_in_milliseconds = True
    timestamp_platform_names = [ParamName.TIMESTAMP]

    def prepare_params(self, endpoint=None, params=None):
        resources, platform_params = super().prepare_params(endpoint, params)

        # (SYMBOL was used in URL path) (not necessary)
        if platform_params and ParamName.SYMBOL in platform_params:
            del platform_params[ParamName.SYMBOL]
        return resources, platform_params

    def parse(self, endpoint, data):
        if data and endpoint == Endpoint.SYMBOLS:
            return [item.upper() for item in data]
        return super().parse(endpoint, data)

    def _parse_item(self, endpoint, item_data):
        result = super()._parse_item(endpoint, item_data)

        # Convert Trade.direction
        if result and isinstance(result, Trade) and result.direction:
            # (Can be of "sell"|"buy|"")
            result.direction = Direction.SELL if result.direction == "sell" else \
                (Direction.BUY if result.direction == "buy" else None)

        return result

    def make_url_and_platform_params(self, endpoint=None, params=None, is_join_get_params=False, version=None):
        version = version or self.version

        url = self.base_url.format(version=version) if self.base_url and version else self.base_url
        # Prepare path and params
        url_resources, platform_params = self.prepare_params(endpoint, params)

        # Make resulting URL
        # url=ba://se_url/resou/rces?p=ar&am=s
        if url_resources and url:
            if len(url_resources) > 0:
                url_resources, symbol = url_resources[0].split('/')
            url = urljoin(url + "/", url_resources)+".do"
            if 'symbol' in params:
                url = url + "?symbol=" + symbol

        return url, platform_params



class OkexRESTClient(PrivatePlatformRESTClient):
    platform_id = Platform.OKEX
    version = "1"  # Default version

    IS_NONE_SYMBOL_FOR_ALL_SYMBOLS = True

    _converter_class_by_version = {
        "1": OkexRESTConverterV1,
    }

    def _on_response(self, response, result):
        # super()._on_response(response)

        if not response.ok and "Retry-After" in response.headers:
            self.delay_before_next_request_sec = int(response.headers["Retry-After"])
        else:

            try:
                ratelimit = int(response.headers["x-ratelimit-limit"])
                remaining_requests = float(response.headers["x-ratelimit-remaining"])
                reset_ratelimit_timestamp = int(response.headers["x-ratelimit-reset"])
                if remaining_requests < ratelimit * 0.1:
                    precision_sec = 1  # Current machine time may not precise which can cause ratelimit error
                    self.delay_before_next_request_sec = reset_ratelimit_timestamp - time.time() + precision_sec
                else:
                    self.delay_before_next_request_sec = 0
                self.logger.debug("Ratelimit info. remaining_requests: %s/%s delay: %s",
                                  remaining_requests, ratelimit, self.delay_before_next_request_sec)

            except KeyError as err:
                self.logger.exception(err.__str__)

            except Exception as error:
                self.logger.exception("Error while defining delay_before_next_request_sec.", error)

    def get_symbols(self, version=None):
        # BitMEX has no get_symbols method in API,
        # and None means "all symbols" if defined as symbol param.
        return None

    # If symbol not specified all symbols will be returned
    # todo fetch_latest_trades()
    def fetch_trades(self, symbol=None, limit=None, **kwargs):
        # symbol = None
        return super().fetch_trades(symbol, limit, **kwargs)

    # If symbol not specified all symbols will be returned
    def fetch_trades_history(self, symbol=None, limit=None, from_item=None,
                           sorting=None, from_time=None, to_time=None, **kwargs):
        # Note: from_item used automatically for paging; from_time and to_time - used for custom purposes
        return super().fetch_trades_history(symbol, limit, from_item, sorting=sorting,
                                          from_time=from_time, to_time=to_time, **kwargs)

    # tickers are in instruments