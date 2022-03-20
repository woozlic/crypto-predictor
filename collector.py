import os.path

from binance.client import Client
import pandas as pd

from secret import BINANCE_SECRET_KEY, BINANCE_API_KEY
from constants import TOP_CRYPTO, FIAT, DATE_FORMAT

CURRNENT_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_PATH = os.path.join(CURRNENT_DIR, 'data')


class DataCollector:
    def __init__(self, binance_secret_key: str, binance_api_key: str):
        self._client = Client(binance_secret_key, binance_api_key)

    def get_historical_tickers(self, interval: str, date_from: str, date_to: str):
        for abbreviation in TOP_CRYPTO[:1]:
            symbol = abbreviation + FIAT
            print(f'Getting values for {symbol}')
            candle = self._client.get_historical_klines(symbol, interval, date_from, date_to)
            df = pd.DataFrame(candle, columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime',
                                               'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol',
                                               'takerBuyQuoteVol', 'ignore'])
            df.dateTime = pd.to_datetime(df.dateTime, unit='ms').dt.strftime(DATE_FORMAT)
            df.set_index('dateTime', inplace=True)
            df = df.drop(['closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol',
                          'ignore'], axis=1)
            print(df)
            df_name = f'{symbol}_{interval}_{date_from}_{date_to}.xlsx'
            df_path = os.path.join(DATA_PATH, df_name)
            df.to_excel(df_path)


if __name__ == '__main__':
    data_collector = DataCollector(BINANCE_SECRET_KEY, BINANCE_API_KEY)
    data_collector.get_historical_tickers('1h', '1 Feb, 2022', '1 Mar, 2022')
    data_collector.get_historical_tickers('1d', '1 Feb, 2022', '1 Mar, 2022')
    data_collector.get_historical_tickers('1w', '1 Feb, 2022', '1 Mar, 2022')
