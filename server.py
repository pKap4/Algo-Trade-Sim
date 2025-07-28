import socket
import time
import json
import pandas as pd
from datetime import datetime

HOST = '127.0.0.1'  # localhost
PORT = 65432        # any free port

# Load raw CSV — no cleaning
df_NIFTY_CE250500_31072025 = pd.read_csv('option_data.csv')

# Ensure rows stream oldest to newest
df_NIFTY_CE250500_31072025['DATE '] = pd.to_datetime(df_NIFTY_CE250500_31072025['DATE '], format='%d-%b-%Y')
df_NIFTY_CE250500_31072025['SYMBOL '] = "NIFTY_CE250500_31072025"
df_NIFTY_CE250500_31072025 = df_NIFTY_CE250500_31072025.sort_values(by='DATE ')

# Start TCP server
print(f"[SERVER] Starting server on {HOST}:{PORT}...")
time.sleep(5)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"[SERVER] Listening on {HOST}:{PORT}...")
    time.sleep(5)

    conn, addr = server.accept()
    print(f"[SERVER] Connected to {addr}")

    with conn:
        ctr = 1
        for _, row in df_NIFTY_CE250500_31072025.iterrows():
            data = row.to_dict()

            # Replace 'DATE' with the current timestamp
            data['DATE '] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Convert to JSON string and send
            message = json.dumps(data) + '\n'
            conn.sendall(message.encode('utf-8'))

            #print(f"[SERVER] Sent: {message.strip()}")
            print(f"[SERVER] Data id {ctr} shared at {data['DATE ']}")
            ctr += 1
            time.sleep(3)  # stream one tick per minute

        # Send end of data signal
        
        conn.sendall(b'EOD')  # Indicate end of stream
        print("[SERVER] End of data stream sent.")
        time.sleep(5)  # Allow time for the client to process EOD
