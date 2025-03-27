from mpipe import OrderedWorker, Stage


transform_funcs: dict[str, Stage] = {}


def register(transform_id: str, worker: Stage) -> None:
    """Register a new transform function."""
    transform_funcs[transform_id] = worker


def unregister(transform_id: str) -> None:
    """Unregister a transform function."""
    transform_funcs.pop(transform_id, None)



def get(transform_id: str) -> Stage:
    return transform_funcs[transform_id]
