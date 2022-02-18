import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

CONNECTION_URI = 'postgresql://postgres:postgres@localhost:5438/postgres'


def read_and_enrich_data(engine: Engine) -> pd.DataFrame:
    statement = """
    SELECT adgroups.campaign_id as campaign_id, adgroups.ad_group_id as ad_group_id, adgroups.alias as alias,
           search_items.conversion_value as conversion_value,  search_items.cost as cost
    FROM adgroups
    INNER JOIN search_items 
        ON adgroups.campaign_id = search_items.campaign_id 
            AND adgroups.ad_group_id = search_items.ad_group_id;
    """
    df_aggregations = pd.read_sql(statement, engine)
    df_aggregations['ROAS'] = df_aggregations['conversion_value'] / df_aggregations['cost']
    df_aggregations['country'] = df_aggregations['alias'].apply(lambda x: x.split(' - ')[2])
    df_aggregations['priority'] = df_aggregations['alias'].apply(lambda x: x.split(' - ')[4])
    assert df_aggregations['priority'].isnull().sum() == 0
    assert df_aggregations['country'].isnull().sum() == 0
    return df_aggregations


if __name__ == "__main__":
    engine = create_engine(CONNECTION_URI)
    df_aggregations = read_and_enrich_data(engine=engine)
    print(df_aggregations.groupby(['country', 'priority'])['ROAS'].sum())
