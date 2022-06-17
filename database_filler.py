from datetime import datetime, timedelta

from crud import add_candles_batch, drop_candles_before, get_or_create_exchange, get_or_create_pair
from exchange_connectors import ExchangeConnectorBitfinex, ExchangeConnectorKraken
from schemas import CandleFrequency, CurrencyPair, DatabaseState


BATCH_SIZE = 1000

database_state = DatabaseState.unknown.value


class DatabaseFiller:
    def __init__(self, current_datetime_utc: datetime):
        self.current_datetime_utc = current_datetime_utc

    async def fill_database(self):
        global database_state
        database_state = DatabaseState.filling.value
        try:
            await self._fill_database_impl()
            database_state = DatabaseState.ready.value
        except BaseException:
            database_state = DatabaseState.error.value
            raise

    async def _fill_database_impl(self):
        await self._drop_old_data()
        await self._add_candles()

    async def _drop_old_data(self):
        await drop_candles_before(self.current_datetime_utc, CandleFrequency.hour)
        await drop_candles_before(self.current_datetime_utc, CandleFrequency.minute)

    async def _add_candles(self):
        exchange_connectors = [
            ExchangeConnectorBitfinex,
            ExchangeConnectorKraken,
        ]
        candles = []
        for exchange_connector in exchange_connectors:
            exchange_connector_instance = exchange_connector()
            exchange_from_db = await get_or_create_exchange(exchange_connector_instance.exchange_title)
            for pair in CurrencyPair:
                pair_from_db = await get_or_create_pair(pair.value)
                for candle_frequency in CandleFrequency:
                    if candle_frequency == CandleFrequency.minute:
                        start_utc = self.current_datetime_utc - timedelta(days=1)
                        end_utc = self.current_datetime_utc - timedelta(minutes=1)
                    elif candle_frequency == CandleFrequency.hour:
                        start_utc = self.current_datetime_utc - timedelta(days=30)
                        end_utc = self.current_datetime_utc - timedelta(minutes=1)
                    else:
                        raise Exception('Unsupported candle_frequency {}'.format(candle_frequency))
                    async for candle in exchange_connector_instance.get_minute_candles(pair.value, start_utc, end_utc, candle_frequency):
                        candle['exchange_id'] = exchange_from_db.id
                        candle['pair_id'] = pair_from_db.id
                        candles.append(candle)
                        if len(candles) > BATCH_SIZE:
                            await add_candles_batch(candles)
                            candles.clear()
        if candles:
            await add_candles_batch(candles)
            candles.clear()
