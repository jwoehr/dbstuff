import os
from dotenv import load_dotenv
from mapepire_python import connect

load_dotenv()

creds = {
  "host": os.getenv("host", "localhost"),
  "port": int(os.getenv("port", 8076)),
  "user": os.getenv("user", "user"),
  "password": os.getenv("password", "password"),
  "ignore_unauthorized": "true",
}

with connect(creds) as conn:
    with conn.execute("select * from sample.employee") as cursor:
        result = cursor.fetchone()
        print(result)
    # with conn.execute("select * from dbsdk_v1.conf") as cursor:
        # result = cursor.fetchall()
        # print(result)
    with conn.execute("select json_array (usrprf, watsonx_region) as json_conf_factors from dbsdk_v1.conf") as cursor:
        result = cursor.fetchall()
        for row in result:
            print(row[0])
