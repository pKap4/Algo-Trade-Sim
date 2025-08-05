from collections import deque
from datetime import datetime

class Volume_Fade:
    def __init__(self, volume_window=10, min_gap_percent=0.05):
        self.volume_window = volume_window
        self.min_gap_percent = min_gap_percent
        self.volumes = deque(maxlen=volume_window)
        self.num_ticks = 0
        self.prev_open = None
        self.prev_close = None

    def process(
        self,
        close_price,
        date,
        expiry,
        option_type,
        open_price,
        volume,
        open_interest,
        change_in_oi
    ):
        try:
            #days_to_expiry = (expiry - date).days
            open_p = float(open_price)
            close_p = float(close_price)
            vol = float(volume)
            #oi = float(open_interest)
            coi = float(change_in_oi)
        except Exception as e:
            print(f"[OptionVolumeFade] Error parsing inputs: {e}")
            return None

        self.volumes.append(vol)
        self.num_ticks += 1

        if self.num_ticks < self.volume_window or self.prev_close is None:
            self.prev_open = open_p
            self.prev_close = close_p
            return None

        avg_vol = sum(self.volumes) / self.volume_window
        std_vol = (sum((v - avg_vol) ** 2 for v in self.volumes) / self.volume_window) ** 0.5
        vol_z = (vol - avg_vol) / std_vol if std_vol > 0 else 0

        # Check for gap up
        min_gap = self.prev_close * self.min_gap_percent
        is_gap_up = open_p > self.prev_close + min_gap

        signal = None

        if (
            close_p > open_p and                                         # Green candle
            (close_p - open_p) / open_p > 0.1 and                        # Candle size > 10%
            vol_z < -1.5 and                                              # Not high volume
            #days_to_expiry <= 10 and
            coi <= 0 and                                                # No long buildup
            option_type == "CE" and
            self.prev_close > self.prev_open and                        # Previous candle green
            is_gap_up                                                   # Gap up open
        ):
            target = self.prev_close                                    # Mean reversion to prev close
            stop_loss = close_p + (close_p - open_p)
            reward = close_p - target
            risk = stop_loss - close_p
            rr_ratio = reward / risk if risk > 0 else 0

            if rr_ratio > 1.5:
                signal = "SELL"
                # Update for next tick
                self.prev_open = open_p
                self.prev_close = close_p
                return signal, target, stop_loss

        # Update for next tick
        self.prev_open = open_p
        self.prev_close = close_p

        return None
