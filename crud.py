from datetime import datetime, timedelta, timezone

from database import database
from models import Candle, Pair, Exchange
from schemas import CandleFrequency, CurrencyPair, PairResult


async def add_candles_batch(candles):
    await database.execute_many(query=Candle.__table__.insert(), values=candles)


async def drop_candles_before(day_utc: datetime, candle_frequency: CandleFrequency):
    since = datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc)
    query = Candle.__table__\
        .delete()\
        .filter(Candle.time < int(since.timestamp()))\
        .filter(Candle.frequency == candle_frequency.value)
    await database.execute(query)


async def get_or_create_exchange(exchange_title: str):
    query = Exchange.__table__\
        .select()\
        .filter(Exchange.title == exchange_title)
    res = await database.fetch_one(query=query)
    if res is not None:
        return Exchange(id=res[0], title=res[1])
    query = Exchange.__table__ \
        .insert()\
        .values(title=exchange_title)
    await database.execute(query)
    return await get_or_create_exchange(exchange_title)


async def get_or_create_pair(pair: CurrencyPair):
    query = Pair.__table__\
        .select()\
        .filter(Pair.title == pair)
    res = await database.fetch_one(query=query)
    if res is not None:
        return Pair(id=res[0], title=res[1])
    query = Pair.__table__ \
        .insert()\
        .values(title=pair)
    await database.execute(query)
    return await get_or_create_exchange(pair)


async def get_pairs_by_min_max(pair: CurrencyPair):
    prev_day = datetime.utcnow()

    while True:
        current_day = prev_day
        prev_day = current_day - timedelta(days=1)

        sql = 'SELECT candles.time, candles.close, candles.open, candles.high, candles.low, candles.volume, exchanges.title ' \
              'FROM candles ' \
              'INNER JOIN exchanges ON candles.exchange_id = exchanges.id ' \
              'INNER JOIN pairs ON candles.pair_id = pairs.id ' \
              'WHERE pairs.title = :pair AND candles.frequency = :frequency ' \
              'AND candles.time >= :time_start AND candles.time < :time_end ' \
              'ORDER BY candles.low ' \
              'LIMIT 1'
        values = {'pair': pair.value, 'frequency': CandleFrequency.hour.value,
                  'time_start': int(prev_day.timestamp()), 'time_end': int(current_day.timestamp())}
        res_min = await database.fetch_one(sql, values)
        if not res_min:
            break
        yield PairResult(
            Type='min',
            time=datetime.fromtimestamp(res_min[0], tz=timezone.utc),
            close=res_min[1],
            open=res_min[2],
            high=res_min[3],
            low=res_min[4],
            volume=res_min[5],
            Exchange=res_min[6],
        )

        sql = 'SELECT candles.time, candles.close, candles.open, candles.high, candles.low, candles.volume, exchanges.title ' \
              'FROM candles ' \
              'INNER JOIN exchanges ON candles.exchange_id = exchanges.id ' \
              'INNER JOIN pairs ON candles.pair_id = pairs.id ' \
              'WHERE pairs.title = :pair AND candles.frequency = :frequency ' \
              'AND candles.time >= :time_start AND candles.time < :time_end ' \
              'ORDER BY candles.high DESC ' \
              'LIMIT 1'
        res_max = await database.fetch_one(sql, values)
        if not res_max:
            break
        yield PairResult(
            Type='max',
            time=datetime.fromtimestamp(res_max[0], tz=timezone.utc),
            close=res_max[1],
            open=res_max[2],
            high=res_max[3],
            low=res_max[4],
            volume=res_max[5],
            Exchange=res_max[6],
        )
