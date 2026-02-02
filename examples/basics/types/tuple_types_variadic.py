"""
Example demonstrating variadic tuple inputs and outputs in Flyte tasks.

Variadic tuples like tuple[int, ...] represent tuples of arbitrary length where
all elements have the same type. This is useful when you need an immutable
sequence of homogeneous elements with a known element type.

Key differences from fixed tuples:
- tuple[int, str, float] - exactly 3 elements with specific types
- tuple[int, ...] - any number of int elements (including zero)
"""

from dataclasses import dataclass

import flyte

env = flyte.TaskEnvironment(
    name="tuple_types_variadic_example",
    image=flyte.Image.from_debian_base(),
)


@env.task
async def create_variadic_tuple() -> tuple[int, ...]:
    """Create and return a variadic tuple of integers."""
    return (1, 2, 3, 4, 5)


@env.task
async def sum_variadic_tuple(numbers: tuple[int, ...]) -> int:
    """Sum all integers in a variadic tuple."""
    return sum(numbers)


@env.task
async def filter_variadic_tuple(numbers: tuple[int, ...], threshold: int) -> tuple[int, ...]:
    """Filter a variadic tuple, keeping only values above the threshold."""
    return tuple(n for n in numbers if n > threshold)


@env.task
async def create_string_tuple() -> tuple[str, ...]:
    """Create a variadic tuple of strings."""
    return ("apple", "banana", "cherry", "date")


@env.task
async def join_string_tuple(words: tuple[str, ...], separator: str) -> str:
    """Join all strings in a variadic tuple with a separator."""
    return separator.join(words)


@env.task
async def create_float_tuple(n: int) -> tuple[float, ...]:
    """Create a variadic tuple of floats based on input size."""
    return tuple(float(i) * 0.5 for i in range(n))


@env.task
async def compute_statistics(values: tuple[float, ...]) -> tuple[float, float, float]:
    """Compute min, max, and average of a variadic float tuple.

    Returns a fixed tuple with (min, max, average).
    """
    if not values:
        return (0.0, 0.0, 0.0)
    return (min(values), max(values), sum(values) / len(values))


@dataclass
class Item:
    """A simple item with a name and value."""

    name: str
    value: int


@env.task
async def create_item_tuple() -> tuple[Item, ...]:
    """Create a variadic tuple of dataclass instances."""
    return (
        Item(name="item_a", value=10),
        Item(name="item_b", value=20),
        Item(name="item_c", value=30),
    )


@env.task
async def sum_item_values(items: tuple[Item, ...]) -> int:
    """Sum the values of all items in a variadic tuple."""
    return sum(item.value for item in items)


@env.task
async def variadic_tuple_workflow() -> tuple[
    tuple[int, ...],
    int,
    tuple[int, ...],
    tuple[str, ...],
    str,
    tuple[float, ...],
    tuple[float, float, float],
    tuple[Item, ...],
    int,
]:
    """Workflow demonstrating variadic tuple type usage."""
    # Integer variadic tuple
    int_tuple = await create_variadic_tuple()
    int_sum = await sum_variadic_tuple(numbers=int_tuple)
    print(f"Sum of {int_tuple}: {int_sum}")

    # Filtered variadic tuple
    filtered = await filter_variadic_tuple(numbers=int_tuple, threshold=2)
    print(f"Filtered (>2): {filtered}")

    # String variadic tuple
    str_tuple = await create_string_tuple()
    joined = await join_string_tuple(words=str_tuple, separator=", ")
    print(f"Joined strings: {joined}")

    # Float variadic tuple with statistics
    float_tuple = await create_float_tuple(n=10)
    stats = await compute_statistics(values=float_tuple)
    print(f"Float tuple: {float_tuple}")
    print(f"Statistics (min, max, avg): {stats}")

    # Dataclass variadic tuple
    item_tuple = await create_item_tuple()
    item_sum = await sum_item_values(items=item_tuple)
    print(f"Sum of item values: {item_sum}")

    return (
        int_tuple,
        int_sum,
        filtered,
        str_tuple,
        joined,
        float_tuple,
        stats,
        item_tuple,
        item_sum,
    )


if __name__ == "__main__":
    flyte.init_from_config()

    print("Running variadic tuple workflow...")
    run = flyte.run(variadic_tuple_workflow)
    print(f"Run URL: {run.url}")
    run.wait()
    print("Variadic tuple workflow completed!")
    outputs = run.outputs()
    print(f"Outputs: {outputs}")
