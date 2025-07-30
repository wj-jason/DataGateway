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
    
    new_rows = pd.DataFrame({
        "name": ["Charlie", "Dana"],
        "value": [75, 25]
    })

    bad_rows = pd.DataFrame({
        "name": ["Eve"],
        "value": ["not an int"]  # wrong dtype
    })    

    print("TESTING APPEND")
    dgw.append("alice_and_bob", new_rows)
    df = dgw.get("alice_and_bob")
    print(df)
    meta = dgw.meta("alice_and_bob")
    print(meta)
    print("SUCESS\n")

    print("TESTING DELETE 1")
    dgw.delete("alice_and_bob", lambda x: x['value'] == 25)
    df = dgw.get("alice_and_bob")
    print(df)
    print("SUCCESS\n")

    print("TESTING DELETE 2")
    mask = df['value'] == 75
    dgw.delete("alice_and_bob", mask)
    df = dgw.get("alice_and_bob")
    print(df)
    print("SUCCESS\n")

    print("TESTING DELETE 3")
    dgw.delete("alice_and_bob", lambda x: x['value'] == 0)
    df = dgw.get("alice_and_bob")
    print(df)
    print("SUCCESS\n")

    print("TESTING INVALID DATA")
    dgw.append("alice_and_bob", bad_rows) # fails
