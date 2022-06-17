from datetime import datetime
from enum import Enum
from pydantic import BaseModel
from pydantic.typing import Literal


class CandleFrequency(Enum):
    minute = 1
    hour = 2


class CurrencyPair(str, Enum):
    btc_usd = 'BTC-USD'
    eth_usd = 'ETH-USD'
    xrp_eur = 'XRP-EUR'
    xrp_usd = 'XRP-USD'


class DatabaseState(Enum):
    error = -1
    unknown = 0
    filling = 1
    ready = 2


class PairResult(BaseModel):
    Type: Literal['min', 'max']
    time: datetime
    close: float
    open: float
    high: float
    low: float
    volume: float
    Exchange: str
