def string_to_bool(value: str) -> bool:
    """
    Convert a string representation of a boolean to a boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        value = value.lower()
        if value in [
            "true",
            "t",
            "yes",
            "y",
            "1",
        ]:
            return True
        else:
            return False
    else:
        instance_type = type(value)
        raise ValueError(
            f"Invalid value type: {instance_type}. Expected str, int, or bool."
        )
