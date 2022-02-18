import pandas as pd
from enum import Enum
from tqdm import tqdm
from sqlalchemy import Table, Column, BigInteger, String, MetaData, Float, Date, Integer
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy.types import Text

CONNECTION_URI = 'postgresql://postgres:postgres@localhost:5438/postgres'


class Tables(Enum):
    campaigns = 'campaigns'
    adgroups = 'adgroups'
    search_items = 'search_items'


def create_tables_if_not_exist(engine: Engine) -> None:
    meta = MetaData()
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    if Tables.campaigns not in table_names:
        Table(
            Tables.campaigns.value, meta,
            Column('campaign_id', BigInteger, primary_key=True),
            Column('structure_value', String, primary_key=True),
            Column('status', String, primary_key=True),
        )
    if Tables.adgroups not in table_names:
        Table(
            Tables.adgroups.value, meta,
            Column('campaign_id', BigInteger, primary_key=True),
            Column('ad_group_id', BigInteger, primary_key=True),
            Column('alias', String, primary_key=True),
            Column('status', String, primary_key=True),
        )
    if Tables.search_items not in table_names:
        Table(
            Tables.search_items.value, meta,
            Column('campaign_id', BigInteger, primary_key=True),
            Column('ad_group_id', BigInteger, primary_key=True),
            Column('date', Date, primary_key=True),
            Column('clicks', BigInteger, primary_key=True),
            Column('cost', Float, primary_key=True),
            Column('conversion_value', Integer, primary_key=True),
            Column('conversions', String, primary_key=True),
            Column('search_term', String, primary_key=True),
        )

    meta.create_all(engine)


def create_upsert_query(table_name: str, column_names: list[str]) -> Text:
    values = [f":{v}" for v in column_names]
    return text(
        f'insert into {table_name} ({", ".join(column_names)}) '
        f'values ({", ".join(values)}) on conflict do nothing;'
    )


if __name__ == "__main__":
    engine = create_engine(CONNECTION_URI)
    create_tables_if_not_exist(engine=engine)
    # TODO: We could use pandera to validate schema
    df_campaigns = pd.read_csv('campaigns.csv')
    # Campaigns dataset has many duplicates that don't make sense
    df_campaigns.drop_duplicates(inplace=True)
    # Adgroups dataset has many duplicates that don't make sense
    df_adgroups = pd.read_csv('adgroups.csv')
    df_adgroups.drop_duplicates(inplace=True)
    df_search_terms = pd.read_csv(
        'search_terms.csv'
    )
    df_search_terms.drop_duplicates(inplace=True)
    table_map = [
        (df_campaigns, Tables.campaigns.value),
        (df_adgroups, Tables.adgroups.value),
        (df_search_terms, Tables.search_items.value)
    ]

    with engine.connect() as connection:
        for df, table_name in table_map:
            statement = create_upsert_query(table_name=table_name, column_names=df.columns)
            data = df.to_dict(orient='records')
            for line in tqdm(data):
                connection.execute(statement, **line)
