from DataGateway import DataGateway
import pandas as pd
import json

if __name__ == '__main__':
    with open("config.json", "r") as f:
        config = json.load(f)

    print("TESTING CONNECTION")
    dgw = DataGateway(config)
    print("SUCESS\n")

    print("TESTING PUT")
    dgw.put(
        "alice_and_bob",
        pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "value": [100, 50]
            }
        ),
        overwrite=True
    )
    print("SUCESS\n")

    print("TESTING GET")
    df = dgw.get("alice_and_bob")
    print(df)
    print("SUCESS\n")

    print("TESTING META")
    meta = dgw.meta("alice_and_bob")
    print(meta)
    print("SUCESS\n")

    print("TESTING LIST")
    print(dgw.list())
    print("SUCESS\n")
   
    print("TESTING DELETE")
    dgw.delete("alice_and_bob")
    print("SUCCESS\n")
