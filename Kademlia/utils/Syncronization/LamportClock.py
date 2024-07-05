import threading

lock = threading.Lock()


class LamportClock:
    def __init__(self) -> None:
        self.ticks = 0

    def tick(self):
        with lock:
            self.ticks += 1
            print("the clock ticks ", self.ticks)

    def merge_ticks(self, others_tick: int):
        with lock:
            print("the clock merged ", self.ticks, " with ", others_tick)
            self.ticks = max(self.ticks, others_tick)
