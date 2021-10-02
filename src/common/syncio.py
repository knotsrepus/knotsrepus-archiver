import asyncio


def iterate_synchronously(ait, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    ait = ait.__aiter__()

    async def get_next():
        try:
            obj = await ait.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    while True:
        done, obj = loop.run_until_complete(get_next())
        if done:
            break
        yield obj


def run_synchronously(future, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    return loop.run_until_complete(future)
