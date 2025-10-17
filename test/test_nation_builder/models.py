from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class User:
    """Represents a User object returned by the API."""
    id: int
    username: str
    email: str
    is_active: bool
    joined_date: str