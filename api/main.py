from fastapi import FastAPI
import os
import sqlalchemy
import uvicorn
import psycopg2

app = FastAPI()


@app.get("/")
def read_root():
    database = os.environ.get("DATABASE")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")

    url = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
    engine = sqlalchemy.create_engine(url)

    sql = f"""
    select * 
    from prediction;
    """

    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(sql)

    return query.fetchall()


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
