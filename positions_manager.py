from multiprocessing import Manager, Lock
from collections import deque
import time

class PositionsManager:
    """
    Manages trading positions, market prices, and a trade log across multiple processes.
    Uses multiprocessing.Manager for shared data structures.
    """

    def __init__(self, manager=None):
        """
        Initializes the PositionsManager.
        
        Args:
            manager (multiprocessing.Manager, optional): An existing Manager instance.
                                                         If None, a new one is created.
        """
        if manager is None:
            manager = Manager()
        
        # Shared dictionaries for positions and prices
        # positions: { identifier: deque of position_info_dicts }
        self.positions = manager.dict()  
        # prices: { identifier: current market price }
        self.prices = manager.dict()     
        
        # Shared list for the trade log (Manager handles list appends atomically)
        self.trade_log = manager.list()  
        
        # Shared value for total realized PnL, protected by a lock
        self.realized_pnl = manager.Value('d', 0.0) # 'd' for double (float)
        self._pnl_lock = manager.Lock() # Lock to ensure atomic updates to realized_pnl

    def update_market_price(self, identifier: str, price: float):
        """
        Updates the current market price for an identifier and triggers auto-closing
        of positions for that identifier.

        Args:
            identifier (str): The symbol or unique identifier of the asset.
            price (float): The current market price of the asset.
        """
        self.prices[identifier] = price
        self._auto_close_positions(identifier)

    def update_position(self, identifier: str, signal: str, price: float, target: float, stop_loss: float):
        """
        Opens a new position (long or short) for a given identifier.

        Args:
            identifier (str): The symbol or unique identifier of the asset.
            signal (str): "BUY" for a long position, "SELL" for a short position.
            price (float): The entry price of the position.
            target (float): The target price for the position.
            stop_loss (float): The stop-loss price for the position.
        """
        # Retrieve the deque, convert to a list for local modification.
        # Use .get() with a default deque() to handle new identifiers gracefully.
        current_positions_list = list(self.positions.get(identifier, deque()))

        side = "LONG" if signal == "BUY" else "SHORT"
        pos_data = {
            "side": side,
            "entry": price,
            "target": target,
            "stop": stop_loss,
            "open_time": time.time()
        }
        current_positions_list.append(pos_data)
        
        # Reassign the modified deque (created from the list) back to the manager.dict().
        # This is crucial for the Manager to synchronize the change.
        self.positions[identifier] = deque(current_positions_list)
        
        print(f"[OPEN] {identifier} {side} entered at {price:.2f} (Target: {target:.2f}, Stop: {stop_loss:.2f})")

    def _auto_close_positions(self, identifier: str):
        """
        Automatically closes positions based on current market price hitting target or stop-loss.
        This method is called internally by update_market_price.
        
        Args:
            identifier (str): The symbol or unique identifier of the asset.
        """
        if identifier not in self.positions or identifier not in self.prices:
            return

        price = self.prices[identifier]
        
        # Retrieve the deque, convert to a list for safe, local modification.
        # This creates a local copy to work with.
        current_positions_list = list(self.positions[identifier]) 
        
        closed_trades_info = [] # Temporarily store info for logging and PnL update

        # Iterate through the list. Using a while loop with index for safe removal.
        i = 0
        while i < len(current_positions_list):
            pos = current_positions_list[i]
            side = pos['side']
            entry = pos['entry']
            target = pos['target']
            stop = pos['stop']
            open_time = pos['open_time']

            should_close = False
            if side == "LONG":
                if price >= target:
                    #print(f"DEBUG: {identifier} LONG hit TARGET. Price: {price}, Target: {target}")
                    should_close = True
                elif price <= stop:
                    #print(f"DEBUG: {identifier} LONG hit STOP. Price: {price}, Stop: {stop}")
                    should_close = True
            elif side == "SHORT":
                if price <= target: # For short, target is lower than entry
                    #print(f"DEBUG: {identifier} SHORT hit TARGET. Price: {price}, Target: {target}")
                    should_close = True
                elif price >= stop: # For short, stop is higher than entry
                    #print(f"DEBUG: {identifier} SHORT hit STOP. Price: {price}, Stop: {stop}")
                    should_close = True

            if should_close:
                # Remove the position from the local list
                closed_pos = current_positions_list.pop(i) 
                close_time = time.time()
                pnl = self._calculate_pnl(side, entry, price)
                
                # Acquire lock before updating the shared realized_pnl value
                with self._pnl_lock:
                    self.realized_pnl.value += pnl # Access the shared value with .value
                
                trade_info = {
                    "identifier": identifier,
                    "side": side,
                    "entry": entry,
                    "exit": price,
                    "pnl": pnl,
                    "entry_time": open_time,
                    "exit_time": close_time
                }
                self.trade_log.append(trade_info) # manager.list() handles appending atomically
                closed_trades_info.append(trade_info)
                # Do NOT increment 'i' here, as the list elements shifted left
            else:
                i += 1 # Only move to the next element if no item was removed

        # After all local modifications, reassign the updated deque back to the manager.dict().
        # This is absolutely essential for changes to be reflected across processes.
        self.positions[identifier] = deque(current_positions_list)

        for closed in closed_trades_info:
            print(f"[CLOSE] {closed['identifier']} {closed['side']} exited at {closed['exit']:.2f} (entry: {closed['entry']:.2f}, PnL: {closed['pnl']:.2f})")

    def _calculate_pnl(self, side: str, entry: float, exit: float) -> float:
        """
        Calculates the Profit and Loss for a single trade.

        Args:
            side (str): "LONG" or "SHORT".
            entry (float): Entry price.
            exit (float): Exit price.

        Returns:
            float: The calculated PnL.
        """
        if side == "LONG":
            return exit - entry
        elif side == "SHORT":
            return entry - exit
        return 0.0

    def get_trade_log(self) -> list:
        """
        Retrieves a copy of the complete trade log.

        Returns:
            list: A list of dictionaries, where each dictionary represents a closed trade.
        """
        return list(self.trade_log) # Return a copy

    def get_realized_pnl(self) -> float:
        """
        Retrieves the total realized Profit and Loss across all trades.

        Returns:
            float: The aggregate realized PnL.
        """
        return self.realized_pnl.value # Access the shared value via .value