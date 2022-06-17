from abc import ABC
from aiohttp import ClientSession
from datetime import datetime, timedelta

from schemas import CandleFrequency, CurrencyPair


class ExchangeConnectorBase(ABC):
    @property
    def exchange_title(self):
        raise NotImplementedError()

    async def get_minute_candles(self, currency_pair: CurrencyPair, start_utc: datetime, end_utc: datetime, candle_frequency: CandleFrequency):
        raise NotImplementedError()


class ExchangeConnectorBitfinex(ExchangeConnectorBase):
    def __init__(self):
        self.pairs = {
            CurrencyPair.btc_usd: 'tBTCUSD',
            CurrencyPair.eth_usd: 'tETHUSD',
            CurrencyPair.xrp_eur: 'tXRPEUR',
            CurrencyPair.xrp_usd: 'tXRPUSD',
        }

    @property
    def exchange_title(self):
        return 'Bitfinex'

    async def get_minute_candles(self, currency_pair: CurrencyPair, start_utc: datetime, end_utc: datetime, candle_frequency: CandleFrequency):
        pair_symbol = self.pairs[currency_pair]
        if candle_frequency == CandleFrequency.minute:
            interval = '1m'
        elif candle_frequency == CandleFrequency.hour:
            interval = '1h'
        else:
            raise Exception('Unsupported interval')
        url = 'https://api-pub.bitfinex.com/v2/candles/trade:{interval}:{pair_symbol}/hist?limit=10000&start={start}&end={end}&sort=1'.format(
            pair_symbol=pair_symbol,
            start=int(start_utc.timestamp()*1000),
            end=int(end_utc.timestamp()*1000),
            interval=interval,
        )
        async with ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception('Got {} status'.format(response.status))
                res = await response.json()
                for item in res:
                    yield {
                        'frequency': candle_frequency.value,
                        'time': item[0] // 1000,
                        'open': float(item[1]),
                        'close': float(item[2]),
                        'high': float(item[3]),
                        'low': float(item[4]),
                        'volume': float(item[5]),
                    }


class ExchangeConnectorKraken(ExchangeConnectorBase):
    def __init__(self):
        self.pairs = {
            CurrencyPair.btc_usd: 'TBTCUSD',
            CurrencyPair.eth_usd: 'ETHUSD',
            CurrencyPair.xrp_eur: 'XRPEUR',
            CurrencyPair.xrp_usd: 'XRPUSD',
        }

    @property
    def exchange_title(self):
        return 'Kraken'

    async def get_minute_candles(self, currency_pair: CurrencyPair, start_utc: datetime, end_utc: datetime, candle_frequency: CandleFrequency):
        pair_symbol = self.pairs[currency_pair]
        # Actually I have no idea how to make Kraken to honor 'since' parameter and get more than 720 items.
        # See for more info: https://stackoverflow.com/q/48508150/5234527
        if candle_frequency == CandleFrequency.minute:
            interval = 5
        elif candle_frequency == CandleFrequency.hour:
            interval = 240
        else:
            raise Exception('Unsupported interval')
        url = 'https://api.kraken.com/0/public/OHLC?pair={pair_symbol}&since={start}&interval={interval}'.format(
            pair_symbol=pair_symbol,
            start=int(start_utc.timestamp()),
            interval=interval,
        )
        async with ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception('Got {} status'.format(response.status))
                res = await response.json()
                if res.get('error'):
                    raise Exception(res['error'][0])
                for key, value in res['result'].items():
                    if key == 'last':
                        continue
                    for item in value:
                        yield {
                            'frequency': candle_frequency.value,
                            'time': item[0],
                            'open': float(item[1]),
                            'high': float(item[2]),
                            'low': float(item[3]),
                            'close': float(item[4]),
                            'volume': float(item[6]),
                        }


# use for debug only
async def main():
    utc_now = datetime.utcnow()
    start_utc = utc_now - timedelta(days=30)
    end_utc = utc_now - timedelta(minutes=1)

    b = ExchangeConnectorBitfinex()
    counter = 0
    async for item in b.get_minute_candles(CurrencyPair.xrp_usd, start_utc, end_utc, CandleFrequency.hour):
        counter += 1
        timestamp = item['time']
        time = datetime.fromtimestamp(timestamp)
        print(counter, time, item)

    k = ExchangeConnectorKraken()
    counter = 0
    async for item in k.get_minute_candles(CurrencyPair.xrp_eur, start_utc, end_utc, CandleFrequency.hour):
        counter += 1
        timestamp = item['time']
        time = datetime.fromtimestamp(timestamp)
        print(counter, time, item)

# use for debug only
if __name__ == '__main__':
    from asyncio import get_event_loop

    loop = get_event_loop()
    loop.run_until_complete(main())
