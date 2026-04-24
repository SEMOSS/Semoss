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

    def create_thinking_chunk(content: str) -> dict:
        """
        Create a thinking/reasoning chunk.

        Args:
            content: The content thinking chunk to stream

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"thinking": content}
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

    def create_media_chunk(media_info: dict, partial_image_index: int = None) -> dict:
        """
        Create a streaming chunk for image/media output.

        Args:
            media_info: The media descriptor (fileName, base64Data, fileFormat,
                mimeType, mediaInputType) for a partial or final image.
            partial_image_index: 0-based index for streaming partials; omit or
                pass None for the final (completed) image.

        Returns:
            A dictionary representing the streaming chunk
        """
        chunk = {"media_info": media_info}
        if partial_image_index is not None:
            chunk["partial_image_index"] = partial_image_index
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
