class StreamUtil:
    def create_tool_id_chunk(index: int, tool_id: str) -> dict:
        """
        Create a streaming chunk for tool call ID.

        Args:
            index: Tool call index
            tool_id: The tool call ID (e.g., "call_abc123")

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"index": index, "id": tool_id}
        return chunk

    def create_tool_type_chunk(index: int, tool_type: str = "function") -> dict:
        """
        Create a streaming chunk for tool call type.

        Args:
            index: Tool call index
            tool_type: The tool type (usually "function")

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"index": index, "type": tool_type}
        return chunk

    def create_function_name_chunk(index: int, function_name: str) -> dict:
        """
        Create a streaming chunk for function name.

        Args:
            index: Tool call index
            function_name: The function name (e.g., "get_weather")

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"index": index, "function": {"name": function_name}}
        return chunk

    def create_function_arguments_chunk(index: int, arguments_chunk: str) -> dict:
        """
        Create a streaming chunk for function arguments.

        Args:
            index: Tool call index
            arguments_chunk: Partial arguments string (e.g., '{"location"')

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"index": index, "function": {"arguments": arguments_chunk}}
        return chunk

    def create_content_chunk(content: str) -> dict:
        """
        Create a streaming chunk for regular content (non-tool).

        Args:
            content: The content chunk to stream

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"content": content}
        return chunk

    def create_finish_reason_chunk(finish_reason: str = "tool_calls") -> dict:
        """
        Create a streaming chunk to indicate completion.

        Args:
            finish_reason: Reason for finishing ("tool_calls", "stop", etc.)

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"finish_reason": finish_reason}
        return chunk
