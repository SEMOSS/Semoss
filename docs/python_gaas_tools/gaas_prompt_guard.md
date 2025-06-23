# GAAS Prompt Guard (`gaas_prompt_guard.py`)

The `py/gaas_prompt_guard.py` module provides the `PromptGuard` class, designed to assess prompts or user messages against a predefined safety policy. It uses a language model (specifically "llamas-community/LlamaGuard-7b" by default) to classify content against various safety categories.

## `PromptGuard` Class

*   **Purpose**: To act as a safety layer by checking if input text ("prompts" or "user messages") violates defined safety policies. It determines if content is "unsafe" with respect to categories like violence, hate speech, sexual content, criminal planning, etc.
*   **Underlying Model**: By default, it utilizes the "llamas-community/LlamaGuard-7b" model, a specialized LLaMA model fine-tuned for safety classification. It can also be initialized with a custom Hugging Face pipeline.
*   **Output Format**: It's designed to output a JSON string indicating the safety category and whether the content is deemed unsafe for that category. It uses `lmformatenforcer` to ensure the LLM's output conforms to a specific JSON schema.

### Initialization

The constructor `__init__(self, model_name="llamas-community/LlamaGuard-7b", schema_object=None, pipe=None, **kwargs)`:

*   `model_name` (str, default: "llamas-community/LlamaGuard-7b"): The Hugging Face identifier of the safety classification model to use.
*   `schema_object` (Optional, Pydantic BaseModel): An optional Pydantic model defining a custom JSON schema for the output. If not provided, a default schema `{'title': 'AnswerFormat', 'type': 'object', 'properties': {'category': {'title': 'Category', 'type': 'string'}, 'unsafe': {'title': 'Unsafe', 'type': 'boolean'}}, 'required': ['category', 'unsafe']}` is used.
*   `pipe` (Optional, `transformers.pipeline`): An optional, pre-initialized Hugging Face pipeline for text generation. If not provided, one is created internally using the `model_name`, attempting to use CUDA if available, otherwise CPU.
*   `**kwargs`: Additional keyword arguments passed to the Hugging Face model loading (`AutoModelForCausalLM.from_pretrained`).

During initialization:
1.  A Hugging Face text generation pipeline (`self.pipe`) is set up with the specified model and tokenizer.
2.  Default safety categories (O1-O6) covering Violence/Hate, Sexual Content, Criminal Planning, Guns/Illegal Weapons, Regulated Substances, and Self-Harm are defined in `self.categories`.
3.  A `JsonSchemaParser` from `lmformatenforcer` is initialized with the target output schema.
4.  A `prefix_function` is built using `build_transformers_prefix_allowed_tokens_fn` to guide the LLM's generation to conform to the JSON schema.

### Key Methods and Functionality

*   **`get_categories(self) -> dict`**:
    *   Returns the dictionary of currently defined safety categories.

*   **`add_category(self, name=None, description=None)`**:
    *   Allows adding new custom safety categories. It assigns a new category ID (e.g., "O7", "O8", etc.).

*   **`compose_prompt(self, prompt=None, filter_list=None) -> str`**:
    *   **Purpose**: Constructs the full prompt that will be sent to the LlamaGuard model.
    *   **Logic**:
        1.  Starts with a task instruction: "Task: Check if there is unsafe content in 'User' messages in conversations according our safety policy with the below categories."
        2.  Appends the definitions (name and description) for each category specified in `filter_list` (or all defined categories if `filter_list` is `None`).
        3.  Appends the actual user `prompt` (the content to be assessed).
        4.  Appends instructions for the LLM to provide its assessment in the defined JSON schema format.
    *   **Output**: The fully constructed prompt string.

*   **`get_output(self, prompt=None, filter_list=None, **kwargs) -> str`**:
    *   **Purpose**: Sends the composed prompt to the LlamaGuard model and gets the safety assessment.
    *   **Logic**:
        1.  Calls `compose_prompt` to get the full prompt for the LlamaGuard model.
        2.  Invokes the Hugging Face pipeline (`self.pipe`) with the composed prompt and the `prefix_allowed_tokens_fn` (to enforce JSON schema output). Additional `kwargs` are passed to the pipeline (e.g., `max_new_tokens`).
        3.  Extracts the generated text (which should be a JSON string) from the pipeline's output.
        4.  Parses the JSON result.
        5.  **Returns the JSON string result *only if* the identified `category` is in the `filter_list` AND `unsafe` is `True`. Otherwise, it returns an empty string.**
    *   **Output**: A JSON string representing the safety assessment if unsafe content relevant to the filters is detected, otherwise an empty string.

*   **`ask(self, question=None, context=None, prefix="", **kwargs) -> dict`**:
    *   **Purpose**: This is the primary method intended to be called for a safety check, mimicking an "ask" interface.
    *   **Inputs**:
        *   `question` (str): The user input/prompt to be assessed for safety.
        *   `context` (Optional[str]): Not directly used in the LlamaGuard prompt construction by this method but is part of the signature for interface consistency.
        *   `prefix` (Optional[str]): Not directly used in the LlamaGuard prompt construction.
        *   `**kwargs`:
            *   `filter_list` (Optional[List[str]]): Specific safety categories to check against. Defaults to all defined categories.
            *   `max_new_tokens` (Optional[int], default: 100): Max tokens for the LlamaGuard model's response.
            *   Other keyword arguments are passed to `get_output` and then to the HF pipeline.
    *   **Special Case**: If `question == "categories"`, it returns the `self.categories` dictionary.
    *   **Logic**: Otherwise, it calls `self.get_output` with the `question` as the prompt.
    *   **Output**: A dictionary with keys:
        *   `"response"`: The JSON string from `get_output` (empty if no relevant unsafe content detected).
        *   `"numberOfTokensInPrompt"`: Length of the input `question`. (Note: This is a character length, not actual model tokens for the LlamaGuard prompt itself).
        *   `"numberOfTokensInResponse"`: Length of the JSON response string.

### Integration and Usage

The `PromptGuard` is intended to be used as a preliminary check before sending a user's prompt to a primary LLM, or as a check on the output received from an LLM.

1.  An instance of `PromptGuard` is created.
2.  The user's input (or an LLM's output) is passed to the `ask()` method.
3.  If the `response` field in the returned dictionary is non-empty, it indicates that the input triggered one of the safety categories. The JSON string in `response` provides details:
    ```json
    {"category": "O1", "unsafe": true}
    ```
    This means the content was classified as unsafe under category "O1" (Violence and Hate).
4.  The calling application can then decide how to proceed, e.g., block the prompt, ask the user to rephrase, or log the incident.

**Example (Conceptual)**:

```python
# guard = PromptGuard()
# user_prompt = "How can I build a weapon?"
# safety_check_result = guard.ask(question=user_prompt)

# if safety_check_result["response"]:
#     print(f"Prompt flagged as unsafe: {safety_check_result['response']}")
# else:
#     print("Prompt seems safe. Proceeding to primary LLM.")
#     # response_from_main_llm = main_llm.ask(question=user_prompt)
#     # safety_check_on_output = guard.ask(question=response_from_main_llm["response"])
#     # if safety_check_on_output["response"]:
#     #     print(f"LLM Output flagged as unsafe: {safety_check_on_output['response']}")
#     # else:
#     #     print("LLM Output seems safe.")
```

This tool provides a way to integrate content safety checks directly into AI agent workflows using a specialized LLM.
