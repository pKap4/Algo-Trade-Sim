from multiprocessing import Manager, Lock
from collections import deque
import time

class PositionsManager:

    def __init__(self, manager=None, shared_positions=None, shared_prices=None, 
                 shared_trade_log=None, shared_realized_pnl=None, shared_pnl_lock=None):
        """Initializes the PositionsManager."""

        if manager is None:
            manager = Manager()
        
        # Use provided shared objects or create new ones
        self.positions = shared_positions if shared_positions is not None else manager.dict()
        self.prices = shared_prices if shared_prices is not None else manager.dict()
        self.trade_log = shared_trade_log if shared_trade_log is not None else manager.list()
        self.realized_pnl = shared_realized_pnl if shared_realized_pnl is not None else manager.Value('d', 0.0)
        self._pnl_lock = shared_pnl_lock if shared_pnl_lock is not None else manager.Lock()

    def update_market_price(self, identifier: str, price: float, name:str = None):
        """
        Updates the current market price for an identifier and triggers auto-closing
        of positions for that identifier.
        """
        self.prices[identifier] = price
        self._auto_close_positions(identifier)

    def update_position(self, identifier: str, signal: str, price: float, target: float, stop_loss: float):
        """Opens a new position (long or short) for a given identifier."""

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
        """Automatically closes positions based on current market price hitting target or stop-loss."""
        
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
        if side == "LONG":
            return exit - entry
        elif side == "SHORT":
            return entry - exit
        return 0.0

    def get_trade_log(self) -> list:
        return list(self.trade_log) # Return a copy

    def get_realized_pnl(self) -> float:
        return self.realized_pnl.value # Access the shared value via .value