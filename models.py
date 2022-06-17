from sqlalchemy import BigInteger, Column, Float, ForeignKey, Integer, SmallInteger, String
from sqlalchemy.orm import relationship

from database import Base


class Exchange(Base):
    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, unique=True)

    candles = relationship("Candle", back_populates="exchange")


class Pair(Base):
    __tablename__ = "pairs"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, unique=True)

    candles = relationship("Candle", back_populates="pair")


class Candle(Base):
    __tablename__ = "candles"

    id = Column(Integer, primary_key=True, index=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    pair_id = Column(Integer, ForeignKey("pairs.id"))
    frequency = Column(SmallInteger)

    time = Column(BigInteger, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    exchange = relationship("Exchange", back_populates="candles")
    pair = relationship("Pair", back_populates="candles")
