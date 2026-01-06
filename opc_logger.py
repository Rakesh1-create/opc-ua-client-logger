from opcua import Client
from datetime import datetime
import time, os, math
import pandas as pd

SERVER_URL = "opc.tcp://LAPTOP-FRV8HPM7:48020"

# --- 10 TAG NODE IDS (from your UAExpert screen) ---
TAGS = {
    "Int32":       "ns=4;s=Demo.Static.Scalar.Int32",
    "Int16":       "ns=4;s=Demo.Static.Scalar.Int16",
    "Integer":     "ns=4;s=Demo.Static.Scalar.Integer",
    "Number":      "ns=4;s=Demo.Static.Scalar.Number",
    "NodeId":      "ns=4;s=Demo.Static.Scalar.NodeId",
    "ImagePNG":    "ns=4;s=Demo.Static.Scalar.ImagePNG",
    "HeaterStatus":"ns=4;s=Demo.Static.Scalar.HeaterStatus",
    "Guid":        "ns=4;s=Demo.Static.Scalar.Guid",
    "Float":       "ns=4;s=Demo.Static.Scalar.Float",
    "Double":      "ns=4;s=Demo.Static.Scalar.Double",
}

READ_INTERVAL_SEC = 60   # once per minute

def get_hour_filename():
    now = datetime.now()
    return f"OPC_Log_{now:%Y-%m-%d_%H}.csv"

def main():
    client = Client(SERVER_URL)
    client.connect()
    print("Connected to OPC UA Server")

    nodes = {name: client.get_node(nid) for name, nid in TAGS.items()}

    current_file = None
    buffer = []

    while True:
        now = datetime.now()
        epoch = math.floor(time.time())
        filename = get_hour_filename()

        # rotate file when hour changes
        if current_file != filename and buffer:
            pd.DataFrame(buffer).to_csv(current_file, index=False)
            buffer = []

        current_file = filename

        row = {
            "Timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "EpochUTC": epoch,
        }

        # read all tag values
        for name, node in nodes.items():
            try:
                row[name] = node.get_value()
            except Exception:
                row[name] = None

        buffer.append(row)

        # write progressively
        pd.DataFrame(buffer).to_csv(current_file, index=False)

        print("Logged:", row)

        time.sleep(READ_INTERVAL_SEC)


if __name__ == "__main__":
    main()
