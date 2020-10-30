from typing import Dict, Type

import aiohttp

from polygon.rest import models
from polygon.rest.models import unmarshal


class AsyncRESTClient:
    """ This is a custom generated class """

    DEFAULT_HOST = "api.polygon.io"

    def __init__(self, auth_key: str):
        self.auth_key = auth_key
        self.url = "https://" + self.DEFAULT_HOST

        self._auth = {"apiKey": self.auth_key}
        # self._session = aiohttp.ClientSession()
        # self._session.params["apiKey"] = self.auth_key

    async def _handle_response(
        self, response_type: str, endpoint: str, params: Dict[str, str]
    ) -> Type[models.AnyDefinition]:
        # params["apiKey"] = self.auth_key

        connector = aiohttp.TCPConnector(limit=60)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(endpoint, params=self._auth) as resp:
                if resp.status == 200:
                    resp_json = await resp.json()
                    return unmarshal.unmarshal_json(response_type, resp_json)
                else:
                    resp.raise_for_status()

    async def _handle_response_with_session(
            self, session: aiohttp.ClientSession, response_type: str, endpoint: str, params: Dict[str, str]
    ) -> Type[models.AnyDefinition]:
        """

        :type session: aiohttp.TCPConnector
        """
        async with session.get(endpoint, params=self._auth) as resp:
            if resp.status == 200:
                resp_json = await resp.json()
                return unmarshal.unmarshal_json(response_type, resp_json)
            else:
                resp.raise_for_status()

    async def reference_tickers(
        self, **query_params
    ) -> models.ReferenceTickersApiResponse:
        endpoint = f"{self.url}/v2/reference/tickers"
        return await self._handle_response(
            "ReferenceTickersApiResponse", endpoint, query_params
        )

    async def reference_ticker_types(
        self, **query_params
    ) -> models.ReferenceTickerTypesApiResponse:
        endpoint = f"{self.url}/v2/reference/types"
        return await self._handle_response(
            "ReferenceTickerTypesApiResponse", endpoint, query_params
        )

    async def reference_ticker_details(
        self, symbol, **query_params
    ) -> models.ReferenceTickerDetailsApiResponse:
        endpoint = f"{self.url}/v1/meta/symbols/{symbol}/company"
        return await self._handle_response(
            "ReferenceTickerDetailsApiResponse", endpoint, query_params
        )

    async def reference_ticker_news(
        self, symbol, **query_params
    ) -> models.ReferenceTickerNewsApiResponse:
        endpoint = f"{self.url}/v1/meta/symbols/{symbol}/news"
        return await self._handle_response(
            "ReferenceTickerNewsApiResponse", endpoint, query_params
        )

    async def reference_markets(
        self, **query_params
    ) -> models.ReferenceMarketsApiResponse:
        endpoint = f"{self.url}/v2/reference/markets"
        return await self._handle_response(
            "ReferenceMarketsApiResponse", endpoint, query_params
        )

    async def reference_locales(
        self, **query_params
    ) -> models.ReferenceLocalesApiResponse:
        endpoint = f"{self.url}/v2/reference/locales"
        return await self._handle_response(
            "ReferenceLocalesApiResponse", endpoint, query_params
        )

    async def reference_stock_splits(
        self, symbol, **query_params
    ) -> models.ReferenceStockSplitsApiResponse:
        endpoint = f"{self.url}/v2/reference/splits/{symbol}"
        return await self._handle_response(
            "ReferenceStockSplitsApiResponse", endpoint, query_params
        )

    async def reference_stock_dividends(
        self, symbol, **query_params
    ) -> models.ReferenceStockDividendsApiResponse:
        endpoint = f"{self.url}/v2/reference/dividends/{symbol}"
        return await self._handle_response(
            "ReferenceStockDividendsApiResponse", endpoint, query_params
        )

    async def reference_stock_financials(
        self, symbol, **query_params
    ) -> models.ReferenceStockFinancialsApiResponse:
        endpoint = f"{self.url}/v2/reference/financials/{symbol}"
        return await self._handle_response(
            "ReferenceStockFinancialsApiResponse", endpoint, query_params
        )

    async def reference_market_status(
        self, **query_params
    ) -> models.ReferenceMarketStatusApiResponse:
        endpoint = f"{self.url}/v1/marketstatus/now"
        return await self._handle_response(
            "ReferenceMarketStatusApiResponse", endpoint, query_params
        )

    async def reference_market_holidays(
        self, **query_params
    ) -> models.ReferenceMarketHolidaysApiResponse:
        endpoint = f"{self.url}/v1/marketstatus/upcoming"
        return await self._handle_response(
            "ReferenceMarketHolidaysApiResponse", endpoint, query_params
        )

    async def stocks_equities_exchanges(
        self, **query_params
    ) -> models.StocksEquitiesExchangesApiResponse:
        endpoint = f"{self.url}/v1/meta/exchanges"
        return await self._handle_response(
            "StocksEquitiesExchangesApiResponse", endpoint, query_params
        )

    async def stocks_equities_historic_trades(
        self, symbol, date, **query_params
    ) -> models.StocksEquitiesHistoricTradesApiResponse:
        endpoint = f"{self.url}/v1/historic/trades/{symbol}/{date}"
        return await self._handle_response(
            "StocksEquitiesHistoricTradesApiResponse", endpoint, query_params
        )

    async def historic_trades_v2(
        self, ticker, date, **query_params
    ) -> models.HistoricTradesV2ApiResponse:
        endpoint = f"{self.url}/v2/ticks/stocks/trades/{ticker}/{date}"
        return await self._handle_response(
            "HistoricTradesV2ApiResponse", endpoint, query_params
        )

    async def stocks_equities_historic_quotes(
        self, symbol, date, **query_params
    ) -> models.StocksEquitiesHistoricQuotesApiResponse:
        endpoint = f"{self.url}/v1/historic/quotes/{symbol}/{date}"
        return await self._handle_response(
            "StocksEquitiesHistoricQuotesApiResponse", endpoint, query_params
        )

    async def historic_n___bbo_quotes_v2(
        self, ticker, date, **query_params
    ) -> models.HistoricNBboQuotesV2ApiResponse:
        endpoint = f"{self.url}/v2/ticks/stocks/nbbo/{ticker}/{date}"
        return await self._handle_response(
            "HistoricNBboQuotesV2ApiResponse", endpoint, query_params
        )

    async def stocks_equities_last_trade_for_a_symbol(
        self, symbol, **query_params
    ) -> models.StocksEquitiesLastTradeForASymbolApiResponse:
        endpoint = f"{self.url}/v1/last/stocks/{symbol}"
        return await self._handle_response(
            "StocksEquitiesLastTradeForASymbolApiResponse", endpoint, query_params
        )

    async def stocks_equities_last_quote_for_a_symbol(
        self, symbol, **query_params
    ) -> models.StocksEquitiesLastQuoteForASymbolApiResponse:
        endpoint = f"{self.url}/v1/last_quote/stocks/{symbol}"
        return await self._handle_response(
            "StocksEquitiesLastQuoteForASymbolApiResponse", endpoint, query_params
        )

    async def stocks_equities_daily_open_close(
        self, symbol, date, **query_params
    ) -> models.StocksEquitiesDailyOpenCloseApiResponse:
        endpoint = f"{self.url}/v1/open-close/{symbol}/{date}"
        return await self._handle_response(
            "StocksEquitiesDailyOpenCloseApiResponse", endpoint, query_params
        )

    async def stocks_equities_condition_mappings(
        self, ticktype, **query_params
    ) -> models.StocksEquitiesConditionMappingsApiResponse:
        endpoint = f"{self.url}/v1/meta/conditions/{ticktype}"
        return await self._handle_response(
            "StocksEquitiesConditionMappingsApiResponse", endpoint, query_params
        )

    async def stocks_equities_snapshot_all_tickers(
        self, **query_params
    ) -> models.StocksEquitiesSnapshotAllTickersApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/us/markets/stocks/tickers"
        return await self._handle_response(
            "StocksEquitiesSnapshotAllTickersApiResponse", endpoint, query_params
        )

    async def stocks_equities_snapshot_single_ticker(
        self, ticker, **query_params
    ) -> models.StocksEquitiesSnapshotSingleTickerApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        return await self._handle_response(
            "StocksEquitiesSnapshotSingleTickerApiResponse", endpoint, query_params
        )

    async def stocks_equities_snapshot_gainers_losers(
        self, direction, **query_params
    ) -> models.StocksEquitiesSnapshotGainersLosersApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/us/markets/stocks/{direction}"
        return await self._handle_response(
            "StocksEquitiesSnapshotGainersLosersApiResponse", endpoint, query_params
        )

    async def stocks_equities_previous_close(
        self, ticker, **query_params
    ) -> models.StocksEquitiesPreviousCloseApiResponse:
        endpoint = f"{self.url}/v2/aggs/ticker/{ticker}/prev"
        return await self._handle_response(
            "StocksEquitiesPreviousCloseApiResponse", endpoint, query_params
        )

    async def stocks_equities_aggregates(
        self, session, ticker, multiplier, timespan, from_, to, **query_params
    ) -> models.StocksEquitiesAggregatesApiResponse:
        endpoint = f"{self.url}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_}/{to}"
        return await self._handle_response_with_session(session=session, response_type="StocksEquitiesAggregatesApiResponse", endpoint=endpoint, params=query_params
        )

    async def stocks_equities_grouped_daily(
        self, locale, market, date, **query_params
    ) -> models.StocksEquitiesGroupedDailyApiResponse:
        endpoint = f"{self.url}/v2/aggs/grouped/locale/{locale}/market/{market}/{date}"
        return await self._handle_response(
            "StocksEquitiesGroupedDailyApiResponse", endpoint, query_params
        )

    async def forex_currencies_historic_forex_ticks(
        self, from_, to, date, **query_params
    ) -> models.ForexCurrenciesHistoricForexTicksApiResponse:
        endpoint = f"{self.url}/v1/historic/forex/{from_}/{to}/{date}"
        return await self._handle_response(
            "ForexCurrenciesHistoricForexTicksApiResponse", endpoint, query_params
        )

    async def forex_currencies_real_time_currency_conversion(
        self, from_, to, **query_params
    ) -> models.ForexCurrenciesRealTimeCurrencyConversionApiResponse:
        endpoint = f"{self.url}/v1/conversion/{from_}/{to}"
        return await self._handle_response(
            "ForexCurrenciesRealTimeCurrencyConversionApiResponse",
            endpoint,
            query_params,
        )

    async def forex_currencies_last_quote_for_a_currency_pair(
        self, from_, to, **query_params
    ) -> models.ForexCurrenciesLastQuoteForACurrencyPairApiResponse:
        endpoint = f"{self.url}/v1/last_quote/currencies/{from_}/{to}"
        return await self._handle_response(
            "ForexCurrenciesLastQuoteForACurrencyPairApiResponse",
            endpoint,
            query_params,
        )

    async def forex_currencies_snapshot_all_tickers(
        self, **query_params
    ) -> models.ForexCurrenciesSnapshotAllTickersApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/global/markets/forex/tickers"
        return await self._handle_response(
            "ForexCurrenciesSnapshotAllTickersApiResponse", endpoint, query_params
        )

    async def forex_currencies_snapshot_gainers_losers(
        self, direction, **query_params
    ) -> models.ForexCurrenciesSnapshotGainersLosersApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/global/markets/forex/{direction}"
        return await self._handle_response(
            "ForexCurrenciesSnapshotGainersLosersApiResponse", endpoint, query_params
        )

    async def crypto_crypto_exchanges(
        self, **query_params
    ) -> models.CryptoCryptoExchangesApiResponse:
        endpoint = f"{self.url}/v1/meta/crypto-exchanges"
        return await self._handle_response(
            "CryptoCryptoExchangesApiResponse", endpoint, query_params
        )

    async def crypto_last_trade_for_a_crypto_pair(
        self, from_, to, **query_params
    ) -> models.CryptoLastTradeForACryptoPairApiResponse:
        endpoint = f"{self.url}/v1/last/crypto/{from_}/{to}"
        return await self._handle_response(
            "CryptoLastTradeForACryptoPairApiResponse", endpoint, query_params
        )

    async def crypto_daily_open_close(
        self, from_, to, date, **query_params
    ) -> models.CryptoDailyOpenCloseApiResponse:
        endpoint = f"{self.url}/v1/open-close/crypto/{from_}/{to}/{date}"
        return await self._handle_response(
            "CryptoDailyOpenCloseApiResponse", endpoint, query_params
        )

    async def crypto_historic_crypto_trades(
        self, from_, to, date, **query_params
    ) -> models.CryptoHistoricCryptoTradesApiResponse:
        endpoint = f"{self.url}/v1/historic/crypto/{from_}/{to}/{date}"
        return await self._handle_response(
            "CryptoHistoricCryptoTradesApiResponse", endpoint, query_params
        )

    async def crypto_snapshot_all_tickers(
        self, **query_params
    ) -> models.CryptoSnapshotAllTickersApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/global/markets/crypto/tickers"
        return await self._handle_response(
            "CryptoSnapshotAllTickersApiResponse", endpoint, query_params
        )

    async def crypto_snapshot_single_ticker(
        self, ticker, **query_params
    ) -> models.CryptoSnapshotSingleTickerApiResponse:
        endpoint = (
            f"{self.url}/v2/snapshot/locale/global/markets/crypto/tickers/{ticker}"
        )
        return await self._handle_response(
            "CryptoSnapshotSingleTickerApiResponse", endpoint, query_params
        )

    async def crypto_snapshot_single_ticker_full_book(
        self, ticker, **query_params
    ) -> models.CryptoSnapshotSingleTickerFullBookApiResponse:
        endpoint = (
            f"{self.url}/v2/snapshot/locale/global/markets/crypto/tickers/{ticker}/book"
        )
        return await self._handle_response(
            "CryptoSnapshotSingleTickerFullBookApiResponse", endpoint, query_params
        )

    async def crypto_snapshot_gainers_losers(
        self, direction, **query_params
    ) -> models.CryptoSnapshotGainersLosersApiResponse:
        endpoint = f"{self.url}/v2/snapshot/locale/global/markets/crypto/{direction}"
        return await self._handle_response(
            "CryptoSnapshotGainersLosersApiResponse", endpoint, query_params
        )