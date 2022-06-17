from datetime import datetime
from fastapi import FastAPI, HTTPException
from typing import List

from crud import get_pairs_by_min_max
from database import Base, database, engine
from database_filler import DatabaseFiller
from schemas import CurrencyPair, DatabaseState, PairResult


Base.metadata.create_all(engine)
app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()
    utc_now = datetime.utcnow()
    database_filler = DatabaseFiller(utc_now)
    await database_filler.fill_database()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/", response_model=List[PairResult])
async def root(pair: CurrencyPair):
    from database_filler import database_state
    if database_state == DatabaseState.error.value:
        raise HTTPException(status_code=500, detail='Database is broken state, try restart app')
    elif database_state == DatabaseState.unknown.value:
        raise HTTPException(status_code=503, detail='Database is not ready, try later')
    elif database_state == DatabaseState.filling.value:
        raise HTTPException(status_code=503, detail='Database is filling, try later')
    elif database_state == DatabaseState.ready.value:
        pass  # database is ready, can continue
    else:
        raise HTTPException(status_code=500, detail='Database is in unknown state')

    result = []
    async for item in get_pairs_by_min_max(pair):
        result.append(item)

    return result
