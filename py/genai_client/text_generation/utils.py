def is_openai_native_model(model_name: str) -> bool:
    """
    Determines if a model is a native OpenAI model based on its name pattern.
    Accommodates Azure OpenAI naming patterns and other variations.

    Args:
        model_name (str): The name of the model to check

    Returns:
        bool: True if the model is a native OpenAI model, False otherwise
    """
    model_name_lower = model_name.lower()

    if "azure" in model_name_lower and any(
        x in model_name_lower for x in ["gpt", "davinci", "o1", "o3"]
    ):
        return True

    openai_identifiers = [
        "gpt-",
        "gpt4",
        "gpt4o",
        "gpt-4",
        "gpt-3.5",
        "text-davinci",
        "text-curie",
        "text-babbage",
        "text-ada",
        "davinci",
        "curie",
        "babbage",
        "ada",
        "whisper",
        "tts-",
        "dall-e",
        "o1",
        "o3",
    ]

    for identifier in openai_identifiers:
        if identifier in model_name_lower:
            return True

    if "-vision-" in model_name_lower and (
        "gpt" in model_name_lower or "davinci" in model_name_lower
    ):
        return True

    if ":ft-openai:" in model_name_lower or "ft:gpt" in model_name_lower:
        return True

    if "deployment" in model_name_lower and any(
        x in model_name_lower for x in ["gpt", "davinci", "o1", "o3"]
    ):
        return True

    return False
