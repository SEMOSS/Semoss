from crewai import Agent, LLM


class AgentManager:
    def __init__():
        pass

    def create_llm(
        self,
        model_name: str,
    ) -> LLM:
        """
        Create a new LLM instance with the specified model name.

        Args:
            model_name (str): The name of the model to be used.

        Returns:
            LLM: An instance of the LLM class.
        """
        return LLM(model=model_name)
