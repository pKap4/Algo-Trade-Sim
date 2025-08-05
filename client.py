import asyncio
import multiprocessing
from strategies.bollinger_mean_reversion import Bollinger_Mean_Reversion
from strategies.volume_fade import Volume_Fade
import json
from positions_manager import PositionsManager
from multiprocessing import Manager

STRATEGY_MAP = {
    "BollingerMeanReversion": Bollinger_Mean_Reversion(),
    "VolumeFade": Volume_Fade(),
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

# -------- POSITIONS MANAGER PROCESS --------
def positions_manager_worker(positions_queue, shared_positions, shared_prices, shared_trade_log, shared_realized_pnl, shared_pnl_lock):
    """Dedicated process for handling all position and price updates"""
    
    positions_manager = PositionsManager(
        shared_positions=shared_positions,
        shared_prices=shared_prices,
        shared_trade_log=shared_trade_log,
        shared_realized_pnl=shared_realized_pnl,
        shared_pnl_lock=shared_pnl_lock
    )
    
    print("[PositionsManager] Started positions manager process.")
    
    while True:
        data = positions_queue.get()
        if data == "EOD" or (isinstance(data, dict) and data.get("action") == "EOD"):
            print("[PositionsManager] Received EOD. Exiting.")
            break
            
        action = data['action']
        
        if action == 'update_price':
            identifier = data['identifier']
            price = data['price']
            # This will update price and trigger auto-close if needed
            positions_manager.update_market_price(identifier, price)
            
        elif action == 'open_position':
            identifier = data['identifier']
            signal = data['signal']
            price = data['price']
            target = data['target']
            stop_loss = data['stop_loss']
            strategy_name = data.get('strategy_name', 'Unknown')
            positions_manager.update_position(identifier, signal, price, target, stop_loss)
            print(f"[PositionsManager] Opened position for {strategy_name}: {identifier} {signal} at {price}")

# -------- SUBSCRIBERS --------
def subscriber_worker(name, sub_queue, positions_queue):
    
    strategy = STRATEGY_MAP.get(name)

    if strategy is None:
        print(f"Unknown strategy name: {name}")
        return

    print(f"[{name}] Strategy worker started.")

    while True:
        data = sub_queue.get()
        if data == "EOD":
            print(f"[{name}] Received EOD. Exiting.")
            break

        data = json.loads(data)

        try:
            price = float(data['CLOSE PRICE '])
            identifier = data['SYMBOL ']
            date = data['DATE ']
            expiry = data['EXPIRY DATE ']
            option_type = data['OPTION TYPE ']
            
            open_price = float(data['OPEN PRICE '])
            close_price = float(data['CLOSE PRICE '])
            volume = float(data['Volume '])
            open_interest = float(data['OPEN INTEREST '])
            change_in_oi = float(data['CHANGE IN OI '])
            
            rec_date = data['REC DATE ']

        except Exception as e:
            print(f"[{name}] Error parsing data: {e}")
            continue

        # Send price update to positions manager (centralized)
        positions_queue.put({
            'action': 'update_price',
            'identifier': identifier,
            'price': price
        })

        # Process strategy logic
        result = strategy.process(close_price, date, expiry, option_type, open_price, volume, open_interest, change_in_oi)
        if result is None:
            continue
        signal, target, stop_loss = result

        if signal:
            # Send position update to positions manager (centralized)
            positions_queue.put({
                'action': 'open_position',
                'identifier': identifier,
                'signal': signal,
                'price': price,
                'target': target,
                'stop_loss': stop_loss,
                'strategy_name': name
            })
            print(f"[{name}] Generated signal: {identifier}, {signal}, Target: {target}, Stop: {stop_loss} at {rec_date}")


# -------- ASYNC SOCKET CLIENT --------
async def socket_reader(host, port, publish_queue):
    
    #Asynchronous client to connect to the data server and push data to the publish queue.
    #Includes robust error handling and explicit resource cleanup for async operations.
    
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
    
    # Create shared data structures ONCE
    shared_positions = mp_manager.dict()
    shared_prices = mp_manager.dict()
    shared_trade_log = mp_manager.list()
    shared_realized_pnl = mp_manager.Value('d', 0.0)
    shared_pnl_lock = mp_manager.Lock()

    # Queues
    publish_queue = multiprocessing.Queue()
    positions_queue = multiprocessing.Queue()  # New queue for position updates
    sub_q1 = multiprocessing.Queue()
    sub_q2 = multiprocessing.Queue()
    subscriber_queues = [sub_q1, sub_q2]

    # Pub-sub manager process
    manager_process = multiprocessing.Process(
        target=pubsub_manager, args=(publish_queue, subscriber_queues)
    )
    manager_process.start()

    # Positions manager process (NEW)
    positions_process = multiprocessing.Process(
        target=positions_manager_worker, 
        args=(positions_queue, shared_positions, shared_prices, shared_trade_log, shared_realized_pnl, shared_pnl_lock)
    )
    positions_process.start()

    # Subscribers - now only pass positions_queue
    sub1 = multiprocessing.Process(target=subscriber_worker, args=("BollingerMeanReversion", sub_q1, positions_queue))
    sub2 = multiprocessing.Process(target=subscriber_worker, args=("VolumeFade", sub_q2, positions_queue))
    sub1.start()
    sub2.start()

    # Async socket reader (main process)
    asyncio.run(socket_reader(host, port, publish_queue))

    # Wait for all strategy processes to finish processing
    sub1.join()
    sub2.join()
    print("[Main] All strategy processes completed.")

    # Now signal EOD to positions manager after strategies are done
    positions_queue.put("EOD")
    print("[Main] Sent EOD signal to positions manager.")

    # Join remaining processes
    manager_process.join()
    positions_process.join()
    print("[Main] All processes joined.")

    # Create PositionsManager instance in main process using the same shared data
    positions_manager = PositionsManager(
        shared_positions=shared_positions,
        shared_prices=shared_prices,
        shared_trade_log=shared_trade_log,
        shared_realized_pnl=shared_realized_pnl,
        shared_pnl_lock=shared_pnl_lock
    )
    
    # Debug information
    print(f"[Debug] Shared trade log length: {len(shared_trade_log)}")
    print(f"[Debug] Shared realized PnL: {shared_realized_pnl.value}")
    
    temp = "\n"*2
    print(temp+"[Trade Log Summary]")
    trade_log = positions_manager.get_trade_log()
    
    if not trade_log:
        print("No trades executed.")
    else:
        print(f"Total trades: {len(trade_log)}")
        print(f"Total realized PnL: {positions_manager.get_realized_pnl():.2f}")
        print("-" * 80)
        
        for trade in trade_log:
            print(f"{trade['identifier']} {trade['side']} | Entry: {trade['entry']:.2f} | Exit: {trade['exit']:.2f} | PnL: {trade['pnl']:.2f}")

    temp = "\n"*5
    print(temp+"[Main] All processes exited cleanly.")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()