package prerna.playground;

public class PlaygroundUtils {
	
	public static final String COT_SYSTEM_PROMPT = "You are an expert reasoning assistant that breaks down user queries into sequential steps using available tools and retrieved knowledge context, always formatting your output as valid JSON according to the provided schema.";
	
    public static final String COT_PROMPT_TEMPLATE = """
        You are an expert reasoning assistant. Given available tools and extra context,
        your job is to break the user's query into a step-by-step reasoning plan in JSON.

        Available tools:
        %s

        Extra context (from knowledge base):
        ```
        %s
        ```

        User query:
        %s

        Produce your chain of thought steps as a JSON object according to this schema:
        %s

        Remember:
        - Each step must be type "tool_call" or "llm_action" with appropriate details.
        - Use the extra context provided above when it is relevant.
        - Steps should have incremental step_number and clear rationales.
        - Respond ONLY with the JSON object.
        """;

    public static final String COT_JSON_SCHEMA = """
        {
          "type": "object",
          "properties": {
            "plan_id": { "type": "string" },
            "user_prompt": { "type": "string" },
            "steps": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "step_number": { "type": "integer" },
                  "description": { "type": "string" },
                  "type": { "type": "string", "enum": ["tool_call", "llm_action"] },
                  "details": {
                    "oneOf": [
                      {
                        "type": "object",
                        "required": ["tool_name", "parameters", "rationale"],
                        "properties": {
                          "tool_name": { "type": "string" },
                          "parameters": { "type": "object", "additionalProperties": true },
                          "rationale": { "type": "string" }
                        }
                      },
                      {
                        "type": "object",
                        "required": ["action", "input", "output", "rationale"],
                        "properties": {
                          "action": { "type": "string" },
                          "input": { "type": "string" },
                          "output": { "type": "string" },
                          "rationale": { "type": "string" }
                        }
                      }
                    ]
                  },
                  "status": {
                    "type": "string",
                    "enum": ["pending", "in_progress", "completed", "failed"]
                  },
                  "result": { "type": "object", "nullable": true, "additionalProperties": true }
                },
                "required": ["step_number", "description", "type", "details", "status"]
              }
            }
          },
          "required": ["plan_id", "user_prompt", "steps"]
        }
        """;
}