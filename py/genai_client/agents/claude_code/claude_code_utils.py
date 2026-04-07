import os
from datetime import datetime, timezone


def _build_change_logger(cwd: str):
    """
    Factory that returns a PostToolUse hook callback bound to the given cwd.
    Logs every file-modifying tool call to {cwd}/.claude/logs/change_log.txt.
    """
    log_path = os.path.join(cwd, ".claude", "logs", "change_log.txt")

    async def log_change(input_data: dict, tool_use_id: str | None, context) -> dict:
        tool_name = input_data.get("tool_name", "unknown")
        tool_input = input_data.get("tool_input", {})
        timestamp = datetime.now(timezone.utc).isoformat()

        file_path = (
            tool_input.get("file_path")
            or tool_input.get("path")
            or tool_input.get("file")
            or "N/A"
        )

        if tool_name == "Bash":
            command = tool_input.get("command", "N/A")
            entry = f"[{timestamp}] TOOL={tool_name} CMD={command}\n"
        else:
            description = tool_input.get("description", "")
            entry = f"[{timestamp}] TOOL={tool_name} FILE={file_path}"
            if description:
                entry += f" DESC={description}"
            entry += "\n"

        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(entry)
        except OSError:
            pass

        return {}

    return log_change
