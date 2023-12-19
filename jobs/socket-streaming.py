import socket
import json
import time
import pandas as pd


def stream_to_spark(file_path, host="spark-master", port=9999, chunksize=2):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()

        conn, _ = s.accept()

        with conn:
            try:
                while True:
                    chunks = pd.read_json(file_path, lines=True, chunksize=chunksize)
                    for chunk in chunks:
                        for record in chunk.to_dict(orient="records"):
                            serialize_data = json.dumps(
                                record,
                                default=handle_json_serialize,
                            ).encode("utf-8")
                            conn.send(serialize_data + b"\n")
                            time.sleep(1)
            except (BrokenPipeError, ConnectionResetError):
                print("Client disconnected")
            finally:
                conn.close()


def handle_json_serialize(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)


if __name__ == "__main__":
    stream_to_spark("datasets/yelp_academic_dataset_review.json")
