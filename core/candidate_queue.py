# ===============================
# CORE/CANDIDATE_QUEUE.PY
# ===============================

from collections import deque
from threading import Lock


class CandidateQueue:

    def __init__(self, logger=None):
        self.queue = deque()
        self.lock = Lock()
        self.logger = logger

    def enqueue(self, candidate):

        with self.lock:
            self.queue.append(candidate)

            if self.logger:
                self.logger.info(
                    f"QUEUE_ENQUEUE symbol={candidate.get('symbol')} "
                    f"new_size={len(self.queue)}"
                )

    def get_all(self):

        with self.lock:

            size_before = len(self.queue)
            items = list(self.queue)

            if self.logger:
                if items:
                    self.logger.info(
                        f"QUEUE_GET_ALL count={len(items)} size_before_clear={size_before}"
                    )
                else:
                    self.logger.info("QUEUE_GET_ALL EMPTY")

            self.queue.clear()

            if self.logger:
                self.logger.info(f"QUEUE_CLEARED size_after_clear={len(self.queue)}")

            return items

    # ===============================
    # NEW: Peek without clearing
    # ===============================
    def peek_all(self):

        with self.lock:

            items = list(self.queue)

            if self.logger:
                if items:
                    self.logger.info(
                        f"QUEUE_PEEK_ALL count={len(items)} size_unchanged={len(self.queue)}"
                    )
                else:
                    self.logger.info("QUEUE_PEEK_ALL EMPTY")

            return items

    def clear(self):

        with self.lock:
            size_before = len(self.queue)
            self.queue.clear()

            if self.logger:
                self.logger.info(
                    f"QUEUE_CLEARED_MANUAL size_before={size_before} size_after=0"
                )

    def empty(self):

        with self.lock:
            is_empty = len(self.queue) == 0

            if self.logger:
                self.logger.info(f"QUEUE_EMPTY_CHECK result={is_empty}")

            return is_empty
