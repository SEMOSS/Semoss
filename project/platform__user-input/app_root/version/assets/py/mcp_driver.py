def requestUserInput(title: str = None, questions: list = None) -> str:
    """
    Ask the user structured clarifying questions.
    Args:
        title (str): Optional heading shown above the questions.
        questions (list): 1-3 structured questions to render for the user.
    Returns:
        str: This tool is answered directly by the user in the chat UI (a
        "respond" decision on the paused run) and should never actually
        reach this function. It exists only as a defensive fallback in
        case a caller resolves the call with an execute/approve decision
        instead of routing it through the pause/respond flow.
    """
    return (
        "RequestUserInput is answered by the user in the chat UI, not "
        "executed. This call should have been resolved with a 'respond' "
        "decision instead of running the tool."
    )
