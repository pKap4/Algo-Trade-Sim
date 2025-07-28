import asyncio
import multiprocessing
from strategies.bollinger_mean_reversion import Bollinger_Mean_Reversion
import json
from positions_manager import PositionsManager
from multiprocessing import Manager

STRATEGY_MAP = {
    "BollingerMeanReversion": Bollinger_Mean_Reversion(),
    # Add more strategies here easily
}

# -------- PUB-SUB MANAGER --------
def pubsub_manager(queue, subscriber_queues):
    while True:
        data = queue.get()
        if data == "EOD":
            print("[Manager] Received EOD. Shutting down.")
            for sub_q in subscriber_queues:
                sub_q.put("EOD")
            break
        #print(f"[Manager] Broadcasting: {data}")
        for sub_q in subscriber_queues:
            sub_q.put(data)

# -------- SUBSCRIBERS --------
def subscriber_worker(name, sub_queue, positions_manager):
    
    strategy = STRATEGY_MAP.get(name)

    if strategy is None:
        print(f"Unknown strategy name: {name}")
        return

    while True:
        data = sub_queue.get()
        if data == "EOD":
            print(f"[{name}] Received EOD. Exiting.")
            break

        data = json.loads(data)

        price = float(data['CLOSE PRICE '])
        identifier = data['SYMBOL ']

        # Update market price for auto-close checks
        positions_manager.update_market_price(identifier, price)

        result = strategy.process(price)
        if result is None:
            continue
        signal, target, stop_loss = result

        if signal:
            positions_manager.update_position(identifier, signal, price, target, stop_loss)
            print(f"[{name}] Symbol: {identifier}, Signal: {signal}, Target: {target}, Stop Loss: {stop_loss} for data at time {data['DATE ']}")

# -------- ASYNC SOCKET CLIENT --------
"""async def socket_reader(host, port, publish_queue):
    reader, _ = await asyncio.open_connection(host, port)
    print(f"[Socket Client] Connected to {host}:{port}")

    while True:
        line = await reader.readline()
        if not line:
            continue
        decoded = line.decode().strip()
        #print(f"[Socket Client] Received: {decoded}")
        publish_queue.put(decoded)
        if decoded == "EOD":
            break"""
# -------- ASYNC SOCKET CLIENT --------
async def socket_reader(host, port, publish_queue):
    """
    Asynchronous client to connect to the data server and push data to the publish queue.
    Includes robust error handling and explicit resource cleanup for async operations.
    """
    reader = None # Initialize to None for finally block
    writer = None # Initialize to None for finally block
    
    try:
        reader, writer = await asyncio.open_connection(host, port)
        print(f"[Socket Client] Connected to {host}:{port}")

        while True:
            line = await reader.readline()
            if not line:
                # This often means the server closed the connection or there's no more data
                print("[Socket Client] Server closed the connection or no more data (empty read).")
                break # Exit loop if connection is closed from server side
            
            decoded = line.decode().strip()
            # print(f"[Socket Client] Received: {decoded}") # Uncomment for verbose output
            
            publish_queue.put(decoded) # Put the data into the multiprocessing Queue

            if decoded == "EOD":
                print("[Socket Client] Received EOD signal. Closing connection.")
                break # Signal end of data stream, exit loop
                
    except ConnectionRefusedError:
        print(f"[Socket Client ERROR] Connection refused. Is the data server running on {host}:{port}?")
    except asyncio.CancelledError:
        print("[Socket Client] Task was cancelled. Shutting down gracefully.")
    except Exception as e:
        print(f"[Socket Client ERROR] An unexpected error occurred: {e}")
    finally:
        # Ensure resources are properly closed, even if errors occur
        if writer: # Check if writer object was successfully created
            try:
                writer.close()
                await writer.wait_closed() # Wait for the writer/transport to close gracefully
                print("[Socket Client] Writer closed successfully.")
            except Exception as e:
                print(f"[Socket Client ERROR] Error closing writer: {e}")
        
        # If reader exists and connection is still open (though writer.close should handle),
        # ensure it's closed.
        if reader and not reader.at_eof(): # at_eof() checks if end-of-file was reached (connection closed)
             try:
                 reader.feed_eof() # Signal EOF to the reader protocol
                 # You might not need explicit reader.close() if writer.close() handles the transport
                 # but including for robustness.
                 print("[Socket Client] Reader signaled EOF.")
             except Exception as e:
                 print(f"[Socket Client ERROR] Error handling reader EOF: {e}")
        
        print("[Socket Client] Disconnected from server.")

# -------- MAIN --------
def main():
    host = "127.0.0.1"
    port = 65432  

   # Shared multiprocessing Manager
    mp_manager = Manager()
    positions_manager = PositionsManager(mp_manager)

    # Queues
    publish_queue = multiprocessing.Queue()
    sub_q1 = multiprocessing.Queue()
    #sub_q2 = multiprocessing.Queue()
    #subscriber_queues = [sub_q1, sub_q2]
    subscriber_queues = [sub_q1]  # For now, only one subscriber

    # Pub-sub manager process
    manager_process = multiprocessing.Process(
        target=pubsub_manager, args=(publish_queue, subscriber_queues)
    )
    manager_process.start()

    # Subscribers
    sub1 = multiprocessing.Process(target=subscriber_worker, args=("BollingerMeanReversion", sub_q1, positions_manager))
    #sub2 = multiprocessing.Process(target=subscriber_worker, args=("Strategy-B", sub_q2))
    sub1.start()
    #sub2.start()

    # Async socket reader (main process)
    asyncio.run(socket_reader(host, port, publish_queue))

    # Join processes
    manager_process.join()
    sub1.join()
    #sub2.join()

    print("\n[Trade Log Summary]")
    trade_log = positions_manager.get_trade_log()
    for trade in trade_log:
        print(f"{trade['identifier']} {trade['side']} | Entry: {trade['entry']} | Exit: {trade['exit']} | PnL: {trade['pnl']:.2f}")


    print("[Main] All processes exited cleanly.")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()
