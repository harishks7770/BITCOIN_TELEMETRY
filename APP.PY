import json
import time
import websocket
from azure.eventhub import EventHubProducerClient, EventData

# ==========================================
# CONFIGURATION
# ==========================================
# Replace with your actual Azure Event Hub connection string and hub name
EVENTHUB_CONNECTION_STR = "Endpoint=sb://<your_namespace>.servicebus.windows.net/;SharedAccessKeyName=<key_name>;SharedAccessKey=<your_key_here>""
EVENTHUB_NAME = "<your_eventhub_name>"

# Binance WebSocket Stream URL for live BTCUSDT trades
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Initialize Event Hub Producer Client
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)


def transform_and_send(raw_json_str):
    """
    Parses Binance payload and remaps case-sensitive keys (e/E, t/T, m/M)
    to unique column names so Microsoft Fabric/Stream Analytics can ingest it.
    """
    try:
        raw_data = json.loads(raw_json_str)

        # Remap keys to prevent case collisions in Fabric
        clean_data = {
            "event_type": raw_data.get("e"),
            "event_time": raw_data.get("E"),
            "symbol": raw_data.get("s"),
            "trade_id": raw_data.get("t"),
            "price": raw_data.get("p"),
            "quantity": raw_data.get("q"),
            "trade_time": raw_data.get("T"),
            "is_buyer_maker": raw_data.get("m"),
            "ignore_flag": raw_data.get("M")
        }

        # Convert back to JSON and send batch to Event Hubs
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(clean_data)))
        producer.send_batch(event_data_batch)

        print(
            f"[SENT] Trade ID {clean_data['trade_id']} | Price: {clean_data['price']} | Qty: {clean_data['quantity']}")

    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")


# ==========================================
# WEBSOCKET EVENT HANDLERS
# ==========================================
def on_message(ws, message):
    transform_and_send(message)


def on_error(ws, error):
    print(f"[WS ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS CLOSED] Code: {close_status_code}, Msg: {close_msg}")


def on_open(ws):
    print("[WS CONNECTED] Successfully connected to Binance WebSocket stream.")


# ==========================================
# MAIN EXECUTION LOOP
# ==========================================
if __name__ == "__main__":
    print("Starting Binance to Fabric Event Hub Streamer...")

    # Run with automatic reconnect
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
        except Exception as e:
            print(f"[RECONNECTING] Connection lost due to: {e}. Retrying in 5 seconds...")
            time.sleep(5)
