from enum import Enum


class StringEnum(Enum):
    """Base enum that can be compared directly with strings"""

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)

    def __hash__(self):
        return hash(self.value)

    @classmethod
    def values(cls):
        """Return list of all enum values"""
        return [member.value for member in cls]
