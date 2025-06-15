from PostGresConn import PostgresSQL
from EconomicData.NewsHelperFunc import dfReturner

def main():
    df = dfReturner()
    datas = df.to_dict(orient='records')
    db = PostgresSQL()
    try:
        for data in datas:
            db.InsertData("economicnews", data=data)
    except Exception as e:
        print("ran into error as {e}")