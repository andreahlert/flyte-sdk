"""
Example demonstrating interoperability between Pydantic BaseModels and dataclasses.

Flyte's type engine supports nesting dataclasses inside Pydantic BaseModels and vice versa.
This works because both use JSON/dict as an intermediate representation during serialization.
"""

from dataclasses import dataclass
from typing import List, Optional

from pydantic import BaseModel, Field

import flyte

env = flyte.TaskEnvironment(name="pydantic_with_dataclasses")


# =============================================================================
# Example 1: Dataclass nested inside a Pydantic BaseModel
# =============================================================================


@dataclass
class Address:
    """A simple dataclass representing an address."""

    street: str
    city: str
    zip_code: str
    country: str = "USA"


class Person(BaseModel):
    """A Pydantic model containing a dataclass field."""

    name: str
    age: int
    email: Optional[str] = None
    address: Address  # Dataclass nested in Pydantic


# =============================================================================
# Example 2: Pydantic BaseModel nested inside a dataclass
# =============================================================================


class Metadata(BaseModel):
    """A Pydantic model for document metadata."""

    version: int = Field(default=1, ge=1)
    author: str
    tags: List[str] = Field(default_factory=list)


@dataclass
class Document:
    """A dataclass containing a Pydantic model field."""

    title: str
    content: str
    meta: Metadata  # Pydantic nested in dataclass


# =============================================================================
# Example 3: Complex nesting - both directions
# =============================================================================


class ContactInfo(BaseModel):
    """Pydantic model for contact information."""

    phone: str
    email: str


@dataclass
class Employee:
    """Dataclass with Pydantic field."""

    employee_id: int
    contact: ContactInfo


class Department(BaseModel):
    """Pydantic model containing dataclasses."""

    name: str
    manager: Employee  # Dataclass in Pydantic
    members: List[Employee]  # List of dataclasses in Pydantic


@dataclass
class Company:
    """Dataclass containing Pydantic models and other dataclasses."""

    company_name: str
    headquarters: Address  # Dataclass in dataclass
    departments: List[Department]  # List of Pydantic in dataclass


# =============================================================================
# Flyte Tasks
# =============================================================================


@env.task
async def process_person(person: Person) -> str:
    """Process a Pydantic model containing a dataclass."""
    return f"{person.name} lives at {person.address.street}, {person.address.city}"


@env.task
async def process_document(doc: Document) -> str:
    """Process a dataclass containing a Pydantic model."""
    return f"'{doc.title}' by {doc.meta.author} (v{doc.meta.version})"


@env.task
async def process_company(company: Company) -> str:
    """Process deeply nested Pydantic/dataclass structures."""
    dept_names = [d.name for d in company.departments]
    return f"{company.company_name} has departments: {', '.join(dept_names)}"


@env.task
async def create_company() -> Company:
    """Create and return a complex nested structure."""
    # Create employees with Pydantic contact info
    alice = Employee(
        employee_id=1,
        contact=ContactInfo(phone="555-0101", email="alice@example.com"),
    )
    bob = Employee(
        employee_id=2,
        contact=ContactInfo(phone="555-0102", email="bob@example.com"),
    )
    charlie = Employee(
        employee_id=3,
        contact=ContactInfo(phone="555-0103", email="charlie@example.com"),
    )

    # Create departments (Pydantic) with employees (dataclass)
    engineering = Department(
        name="Engineering",
        manager=alice,
        members=[bob, charlie],
    )
    sales = Department(
        name="Sales",
        manager=bob,
        members=[charlie],
    )

    # Create company (dataclass) with address (dataclass) and departments (Pydantic)
    return Company(
        company_name="Acme Corp",
        headquarters=Address(
            street="123 Main St",
            city="San Francisco",
            zip_code="94102",
        ),
        departments=[engineering, sales],
    )


@env.task
async def main() -> str:
    """Run all examples demonstrating Pydantic/dataclass interop."""
    results = []

    # Example 1: Dataclass in Pydantic
    person = Person(
        name="Alice",
        age=30,
        email="alice@example.com",
        address=Address(
            street="456 Oak Ave",
            city="Seattle",
            zip_code="98101",
        ),
    )
    result1 = await process_person(person=person)
    results.append(f"Person: {result1}")

    # Example 2: Pydantic in dataclass
    doc = Document(
        title="Flyte Best Practices",
        content="...",
        meta=Metadata(
            version=2,
            author="Bob",
            tags=["flyte", "ml", "orchestration"],
        ),
    )
    result2 = await process_document(doc=doc)
    results.append(f"Document: {result2}")

    # Example 3: Complex nesting
    company = await create_company()
    result3 = await process_company(company=company)
    results.append(f"Company: {result3}")

    return "\n".join(results)


if __name__ == "__main__":
    flyte.init_from_config()

    r = flyte.run(main)
    print(f"Run name: {r.name}")
    print(f"Run URL: {r.url}")
    r.wait()
    outputs = r.outputs()
    print(f"\nResults:\n{outputs[0]}")
