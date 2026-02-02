"""
Example demonstrating variadic tuple inputs and outputs with complex types in Flyte tasks.

This example extends the basic variadic tuple example to showcase usage with more
sophisticated types including:
- Lists and Dicts
- Pydantic BaseModel
- Dataclasses
- flyte.io.File
- flyte.io.Dir
- flyte.io.DataFrame
- pandas.DataFrame

Variadic tuples like tuple[SomeComplexType, ...] represent tuples of arbitrary length
where all elements share the same complex type.
"""

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

import flyte
from flyte.io import DataFrame, Dir, File

env = flyte.TaskEnvironment(
    name="tuple_types_variadic_complex_example",
    image=flyte.Image.from_debian_base().with_pip_packages("pandas", "pyarrow"),
)


# =============================================================================
# Dataclass Examples
# =============================================================================


@dataclass
class Person:
    """A person with name and age."""

    name: str
    age: int
    email: str


@env.task
async def create_person_tuple() -> tuple[Person, ...]:
    """Create a variadic tuple of Person dataclasses."""
    return (
        Person(name="Alice", age=30, email="alice@example.com"),
        Person(name="Bob", age=25, email="bob@example.com"),
        Person(name="Charlie", age=35, email="charlie@example.com"),
    )


@env.task
async def filter_adults(persons: tuple[Person, ...], min_age: int) -> tuple[Person, ...]:
    """Filter persons by minimum age."""
    return tuple(p for p in persons if p.age >= min_age)


@env.task
async def get_person_names(persons: tuple[Person, ...]) -> list[str]:
    """Extract names from a tuple of persons."""
    return [p.name for p in persons]


# =============================================================================
# Pydantic BaseModel Examples
# =============================================================================


class Product(BaseModel):
    """A product with name, price, and metadata."""

    name: str
    price: float
    tags: list[str]
    metadata: dict[str, str]


@env.task
async def create_product_tuple() -> tuple[Product, ...]:
    """Create a variadic tuple of Pydantic Product models."""
    return (
        Product(
            name="Laptop",
            price=999.99,
            tags=["electronics", "computers"],
            metadata={"brand": "TechCo", "warranty": "2 years"},
        ),
        Product(
            name="Headphones",
            price=149.99,
            tags=["electronics", "audio"],
            metadata={"brand": "SoundMax", "type": "wireless"},
        ),
        Product(
            name="Keyboard",
            price=79.99,
            tags=["electronics", "accessories"],
            metadata={"brand": "TypeMaster", "layout": "mechanical"},
        ),
    )


@env.task
async def filter_products_by_price(products: tuple[Product, ...], max_price: float) -> tuple[Product, ...]:
    """Filter products by maximum price."""
    return tuple(p for p in products if p.price <= max_price)


@env.task
async def calculate_total_price(products: tuple[Product, ...]) -> float:
    """Calculate total price of all products."""
    return sum(p.price for p in products)


# =============================================================================
# List and Dict Examples with Variadic Tuples
# =============================================================================


@env.task
async def create_list_tuple() -> tuple[list[int], ...]:
    """Create a variadic tuple of integer lists."""
    return (
        [1, 2, 3],
        [4, 5, 6, 7],
        [8, 9],
        [10, 11, 12, 13, 14],
    )


@env.task
async def flatten_list_tuple(lists: tuple[list[int], ...]) -> list[int]:
    """Flatten a variadic tuple of lists into a single list."""
    result = []
    for lst in lists:
        result.extend(lst)
    return result


@env.task
async def create_dict_tuple() -> tuple[dict[str, int], ...]:
    """Create a variadic tuple of string-to-int dictionaries."""
    return (
        {"a": 1, "b": 2},
        {"c": 3, "d": 4, "e": 5},
        {"f": 6},
    )


@env.task
async def merge_dict_tuple(dicts: tuple[dict[str, int], ...]) -> dict[str, int]:
    """Merge a variadic tuple of dicts into a single dict."""
    result = {}
    for d in dicts:
        result.update(d)
    return result


# =============================================================================
# flyte.io.File Examples
# =============================================================================


@env.task
async def create_file_tuple() -> tuple[File, ...]:
    """Create a variadic tuple of Flyte Files."""
    files = []
    for i in range(3):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write(f"Content of file {i}\nLine 2 of file {i}\n")
            files.append(File.from_local(f.name))
    return tuple(await asyncio.gather(*files))


@env.task
async def read_all_files(files: tuple[File, ...]) -> list[str]:
    """Read contents from a variadic tuple of files."""
    contents = []
    for file in files:
        async with file.open("rb") as f:
            contents.append(bytes(await f.read()).decode("utf-8"))
    return contents


@env.task
async def count_file_lines(files: tuple[File, ...]) -> tuple[int, ...]:
    """Count lines in each file of a variadic tuple."""
    counts = []
    for file in files:
        async with file.open("rb") as f:
            counts.append(len(bytes(await f.read()).decode("utf-8").splitlines()))
    return tuple(counts)


# =============================================================================
# flyte.io.Dir Examples
# =============================================================================


@env.task
async def create_dir_tuple() -> tuple[Dir, ...]:
    """Create a variadic tuple of Flyte Directories."""
    dirs = []
    for i in range(2):
        dir_path = Path(tempfile.mkdtemp())
        # Create some files in each directory
        for j in range(3):
            (dir_path / f"file_{j}.txt").write_text(f"Dir {i}, File {j}")
        dirs.append(Dir.from_local(dir_path))
    return tuple(await asyncio.gather(*dirs))


@env.task
async def count_files_in_dirs(dirs: tuple[Dir, ...]) -> tuple[int, ...]:
    """Count files in each directory of a variadic tuple."""
    counts = []
    for d in dirs:
        dir_path = Path(await d.download())
        counts.append(len(list(dir_path.iterdir())))
    return tuple(counts)


@env.task
async def list_all_files(dirs: tuple[Dir, ...]) -> list[str]:
    """List all file names from a variadic tuple of directories."""
    all_files = []
    for d in dirs:
        dir_path = Path(await d.download())
        for f in dir_path.iterdir():
            all_files.append(f.name)
    return all_files


# =============================================================================
# flyte.io.DataFrame Examples
# =============================================================================


@env.task
async def create_flyte_dataframe_tuple() -> tuple[DataFrame, ...]:
    """Create a variadic tuple of Flyte DataFrames."""
    df1 = pd.DataFrame({"name": ["Alice", "Bob"], "score": [95, 87]})
    df2 = pd.DataFrame({"name": ["Charlie", "Diana"], "score": [92, 88]})
    df3 = pd.DataFrame({"name": ["Eve"], "score": [99]})
    return (DataFrame.wrap_df(df1), DataFrame.wrap_df(df2), DataFrame.wrap_df(df3))


@env.task
async def concat_flyte_dataframes(dfs: tuple[DataFrame, ...]) -> DataFrame:
    """Concatenate a variadic tuple of Flyte DataFrames."""
    pd_dfs = []
    for df in dfs:
        # Use val for local dataframes, otherwise fetch from remote
        pd_df = df.val if df.val is not None else await df.open(pd.DataFrame).all()
        pd_dfs.append(pd_df)
    combined = pd.concat(pd_dfs, ignore_index=True)
    return DataFrame.wrap_df(combined)


@env.task
async def get_dataframe_shapes(dfs: tuple[DataFrame, ...]) -> list[tuple[int, int]]:
    """Get shapes of each DataFrame in a variadic tuple."""
    shapes = []
    for df in dfs:
        # Use val for local dataframes, otherwise fetch from remote
        pd_df = df.val if df.val is not None else await df.open(pd.DataFrame).all()
        shapes.append(pd_df.shape)
    return shapes


# =============================================================================
# pandas.DataFrame Examples
# =============================================================================


@env.task
async def create_pandas_dataframe_tuple() -> tuple[pd.DataFrame, ...]:
    """Create a variadic tuple of pandas DataFrames."""
    df1 = pd.DataFrame({"product": ["A", "B"], "quantity": [10, 20], "price": [5.0, 7.5]})
    df2 = pd.DataFrame({"product": ["C", "D", "E"], "quantity": [15, 25, 30], "price": [3.0, 4.5, 6.0]})
    return (df1, df2)


@env.task
async def calculate_total_revenue(dfs: tuple[pd.DataFrame, ...]) -> float:
    """Calculate total revenue from a variadic tuple of DataFrames."""
    total = 0.0
    for df in dfs:
        total += (df["quantity"] * df["price"]).sum()
    return float(total)


@env.task
async def concat_pandas_dataframes(dfs: tuple[pd.DataFrame, ...]) -> pd.DataFrame:
    """Concatenate a variadic tuple of pandas DataFrames."""
    return pd.concat(list(dfs), ignore_index=True)


# =============================================================================
# Mixed Complex Types in Variadic Tuples (Pydantic models only)
# =============================================================================


class CustomerInfo(BaseModel):
    """Customer information as a Pydantic model for nesting in Order."""

    name: str
    age: int
    email: str


class Order(BaseModel):
    """An order with product, quantity, and customer info.

    Note: When nesting complex types, all nested types must be of the same
    serialization family. Here we use Pydantic models throughout.
    """

    order_id: str
    product: Product
    quantity: int
    customer: CustomerInfo


@env.task
async def create_order_tuple() -> tuple[Order, ...]:
    """Create a variadic tuple of orders with nested Pydantic models."""
    products = (
        Product(
            name="Laptop",
            price=999.99,
            tags=["electronics"],
            metadata={"brand": "TechCo"},
        ),
        Product(
            name="Mouse",
            price=29.99,
            tags=["accessories"],
            metadata={"brand": "ClickPro"},
        ),
    )
    customers = (
        CustomerInfo(name="Alice", age=30, email="alice@example.com"),
        CustomerInfo(name="Bob", age=25, email="bob@example.com"),
    )
    return (
        Order(order_id="ORD-001", product=products[0], quantity=1, customer=customers[0]),
        Order(order_id="ORD-002", product=products[1], quantity=2, customer=customers[1]),
        Order(order_id="ORD-003", product=products[0], quantity=1, customer=customers[1]),
    )


@env.task
async def calculate_order_totals(orders: tuple[Order, ...]) -> dict[str, float]:
    """Calculate total value for each order."""
    return {order.order_id: order.product.price * order.quantity for order in orders}


# =============================================================================
# Main Workflow
# =============================================================================


@env.task
async def complex_variadic_tuple_workflow() -> tuple[
    # Dataclass results
    tuple[Person, ...],
    tuple[Person, ...],
    list[str],
    # Pydantic results
    tuple[Product, ...],
    tuple[Product, ...],
    float,
    # List/Dict results
    tuple[list[int], ...],
    list[int],
    tuple[dict[str, int], ...],
    dict[str, int],
    # File results
    tuple[File, ...],
    list[str],
    tuple[int, ...],
    # Dir results
    tuple[Dir, ...],
    tuple[int, ...],
    list[str],
    # Flyte DataFrame results
    tuple[DataFrame, ...],
    DataFrame,
    list[tuple[int, int]],
    # Pandas DataFrame results
    tuple[pd.DataFrame, ...],
    float,
    pd.DataFrame,
    # Order results
    tuple[Order, ...],
    dict[str, float],
]:
    """Comprehensive workflow demonstrating variadic tuples with complex types."""
    print("=" * 60)
    print("DATACLASS EXAMPLES")
    print("=" * 60)

    # Dataclass operations
    persons = await create_person_tuple()
    print(f"Created {len(persons)} persons")
    adults = await filter_adults(persons=persons, min_age=28)
    print(f"Adults (age >= 28): {len(adults)}")
    names = await get_person_names(persons=persons)
    print(f"Names: {names}")

    print("\n" + "=" * 60)
    print("PYDANTIC BASEMODEL EXAMPLES")
    print("=" * 60)

    # Pydantic operations
    products = await create_product_tuple()
    print(f"Created {len(products)} products")
    affordable = await filter_products_by_price(products=products, max_price=150.0)
    print(f"Affordable products (price <= 150): {len(affordable)}")
    total_price = await calculate_total_price(products=products)
    print(f"Total price: ${total_price:.2f}")

    print("\n" + "=" * 60)
    print("LIST AND DICT EXAMPLES")
    print("=" * 60)

    # List operations
    list_tuple = await create_list_tuple()
    print(f"Created {len(list_tuple)} lists")
    flattened = await flatten_list_tuple(lists=list_tuple)
    print(f"Flattened: {flattened}")

    # Dict operations
    dict_tuple = await create_dict_tuple()
    print(f"Created {len(dict_tuple)} dicts")
    merged = await merge_dict_tuple(dicts=dict_tuple)
    print(f"Merged: {merged}")

    print("\n" + "=" * 60)
    print("FLYTE FILE EXAMPLES")
    print("=" * 60)

    # File operations
    files = await create_file_tuple()
    print(f"Created {len(files)} files")
    contents = await read_all_files(files=files)
    print(f"Read {len(contents)} file contents")
    line_counts = await count_file_lines(files=files)
    print(f"Line counts: {line_counts}")

    print("\n" + "=" * 60)
    print("FLYTE DIR EXAMPLES")
    print("=" * 60)

    # Directory operations
    dirs = await create_dir_tuple()
    print(f"Created {len(dirs)} directories")
    file_counts = await count_files_in_dirs(dirs=dirs)
    print(f"Files per directory: {file_counts}")
    all_files = await list_all_files(dirs=dirs)
    print(f"All files: {all_files}")

    print("\n" + "=" * 60)
    print("FLYTE DATAFRAME EXAMPLES")
    print("=" * 60)

    # Flyte DataFrame operations
    flyte_dfs = await create_flyte_dataframe_tuple()
    print(f"Created {len(flyte_dfs)} Flyte DataFrames")
    combined_flyte_df = await concat_flyte_dataframes(dfs=flyte_dfs)
    combined_pd_df = (
        combined_flyte_df.val if combined_flyte_df.val is not None else await combined_flyte_df.open(pd.DataFrame).all()
    )
    print(f"Combined DataFrame rows: {len(combined_pd_df)}")
    shapes = await get_dataframe_shapes(dfs=flyte_dfs)
    print(f"DataFrame shapes: {shapes}")

    print("\n" + "=" * 60)
    print("PANDAS DATAFRAME EXAMPLES")
    print("=" * 60)

    # Pandas DataFrame operations
    pandas_dfs = await create_pandas_dataframe_tuple()
    print(f"Created {len(pandas_dfs)} pandas DataFrames")
    total_revenue = await calculate_total_revenue(dfs=pandas_dfs)
    print(f"Total revenue: ${total_revenue:.2f}")
    combined_pandas_df = await concat_pandas_dataframes(dfs=pandas_dfs)
    print(f"Combined DataFrame rows: {len(combined_pandas_df)}")

    print("\n" + "=" * 60)
    print("NESTED COMPLEX TYPE EXAMPLES")
    print("=" * 60)

    # Order operations (nested complex types)
    orders = await create_order_tuple()
    print(f"Created {len(orders)} orders")
    order_totals = await calculate_order_totals(orders=orders)
    print(f"Order totals: {order_totals}")

    print("\n" + "=" * 60)
    print("WORKFLOW COMPLETE")
    print("=" * 60)

    return (
        # Dataclass results
        persons,
        adults,
        names,
        # Pydantic results
        products,
        affordable,
        total_price,
        # List/Dict results
        list_tuple,
        flattened,
        dict_tuple,
        merged,
        # File results
        files,
        contents,
        line_counts,
        # Dir results
        dirs,
        file_counts,
        all_files,
        # Flyte DataFrame results
        flyte_dfs,
        combined_flyte_df,
        shapes,
        # Pandas DataFrame results
        pandas_dfs,
        total_revenue,
        combined_pandas_df,
        # Order results
        orders,
        order_totals,
    )


if __name__ == "__main__":
    flyte.init_from_config()

    print("Running complex variadic tuple workflow...")
    run = flyte.run(complex_variadic_tuple_workflow)
    print(f"Run URL: {run.url}")
    run.wait()
    print("Complex variadic tuple workflow completed!")
    outputs = run.outputs()
    print(f"Number of outputs: {len(outputs)}")
