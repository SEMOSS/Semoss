# Model Keys (`model_keys_enum.py`)

The `py/genai_client/model_keys/model_keys_enum.py` file defines the `ModelKeysEnum` class, which standardizes the keys used for common model configuration parameters. This ensures consistency when configuring and interacting with different models and services via the `genai_client`.

## `ModelKeysEnum`

This enumeration (`Enum`) defines common keys that might be expected in configuration dictionaries or passed as parameters when initializing or calling models. Each member has a value (the key string) and an associated description.

### Enum Members

*   **`ModelKeysEnum.api_key`**:
    *   Value: `"api_key"`
    *   Description: "API key being used for the specific model inference"
    *   Usage: Represents the key used to store or pass an API key for authenticating with a model provider's service.

*   **`ModelKeysEnum.api_base`**:
    *   Value: `"api_base"`
    *   Description: "The base URL for the model inference"
    *   Usage: Represents the key for the base URL of the model's API endpoint, especially for self-hosted models or services with configurable endpoints.

*   **`ModelKeysEnum.model`**:
    *   Value: `"model"`
    *   Description: "The name of the model"
    *   Usage: Represents the key for specifying the particular model identifier or name (e.g., "gpt-3.5-turbo", "claude-2", "text-embedding-ada-002").

### Usage

When interacting with components within the `genai_client`, these enum members can be used to ensure the correct keys are used for configuration parameters:

```python
# Example (conceptual)
from py.genai_client.model_keys.model_keys_enum import ModelKeysEnum

config = {
    ModelKeysEnum.api_key.value: "YOUR_API_KEY",
    ModelKeysEnum.model.value: "text-davinci-003"
}

# or to access the value directly
# api_key_str = ModelKeysEnum.api_key # implicitly uses __str__ or .value
# model_name_str = str(ModelKeysEnum.model)

# print(f"Using API key: {config[ModelKeysEnum.api_key]}")
# print(f"Model: {config[ModelKeysEnum.model]}")
```

By using `ModelKeysEnum`, the `genai_client` promotes a more robust and maintainable way to handle common configuration keys, reducing the risk of errors due to typos in string literals.
