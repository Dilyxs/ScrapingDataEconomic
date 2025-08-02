
from NewsHelperFunc import dfReturner
from PostGresConn import PostgresSQL

def main():
    df = dfReturner()
    datas = df.to_dict(orient='records')
    db = PostgresSQL()
    try:
        for data in datas:
            result = db.InsertData("economicnews", data=data)
            if result == 200:
                print("Executed Properly!")
    except Exception as e:
        print(f"ran into error as {e}")


main()