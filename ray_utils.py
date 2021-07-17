import asyncio
import enum

import ray.util
import ray.util.queue


class AdvancedActorPool(ray.util.ActorPool):
    async def map_unordered_queue_async(self, fn, queue, timeout=None):
        while True:
            if not self.has_free():
                yield await self.get_next_unordered_async()
                continue

            try:
                v = await queue.get_async(timeout=timeout)

                if v == QueueSignal.END_OF_QUEUE:
                    break

            except ray.util.queue.Empty:
                # Assume the queue's producer has completed.
                break

            self.submit(fn, v)

        while self.has_next():
            yield await self.get_next_unordered_async()

    async def get_next_unordered_async(self, timeout=None):
        return await asyncio.get_event_loop().run_in_executor(None, lambda: self.get_next_unordered(timeout=timeout))


class QueueSignal(enum.Enum):
    END_OF_QUEUE = enum.auto()
