from collections import deque

class Bollinger_Mean_Reversion:
    def __init__(self, window_size=20, num_std_dev=2):
        self.window_size = window_size
        self.num_std_dev = num_std_dev
        self.prices = deque(maxlen=window_size)
        self.num_ticks = 0

    def process(self, last_close):
        try:
            price = float(last_close)  # assuming format: "SYMBOL,PRICE"
        except Exception as e:
            print(f"[Bollinger_Mean_Reversion] Error processing price: {e}")
            return None
        
        self.prices.append(last_close)
        self.num_ticks += 1

        if self.num_ticks < self.window_size:
            return None  # not enough data

        mean = sum(self.prices) / self.window_size
        std_dev = (sum((x - mean) ** 2 for x in self.prices) / self.window_size) ** 0.5
        upper = mean + self.num_std_dev * std_dev
        lower = mean - self.num_std_dev * std_dev

        signal = None
        target = mean
        stop_loss = None

        if price > upper:
            signal = "SELL"
            stop_loss = upper + std_dev
        elif price < lower:
            signal = "BUY"
            stop_loss = lower - std_dev

        return signal, target, stop_loss