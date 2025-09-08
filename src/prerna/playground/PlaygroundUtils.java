package prerna.playground;

public class PlaygroundUtils {

	public static final String COT_SYSTEM_PROMPT = "You are an expert reasoning assistant that breaks down user queries into sequential steps using available tools and retrieved knowledge context, always formatting your output as valid JSON according to the provided schema.";

	public static final String COT_PROMPT_TEMPLATE = """
			      You are an Expert AI Planning Agent. Your primary function is to create a 
			      comprehensive, step-by-step execution plan. in JSON.
			      
			      
			      # TASK
			      1.  **Analyze Inputs:** Review the user prompt and the enriched context.
			      2.  **Formulate a Plan:** Create a step-by-step plan to fulfill the request.
			      3.  **Assign Actors:** For each step, determine if it should be `tool_call`, `llm_reasoning`, or `human_intervention`.
			      4.  **Identify Gaps:** If a necessary action cannot be performed, create a `no_tool_available` step.
			      5.  **Define Success:** For each step, you MUST define a machine-readable `success_criteria` object.

			      Available tools:
			      %s

			      Extra context (from knowledge base):
			      ```
			      %s
			      ```

			      User query:
			      %s 

			Produce your chain of thought steps as a JSON object.
			Follow the structure below 
			
			
			{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Unified Agent Plan",
  "description": "A complete, step-by-step execution plan with provenance, success criteria, and multi-actor steps.",
  "type": "object",
  "required": ["prompt_context", "execution_plan"],
  "properties": {
    "prompt_context": { "...": "..." },
    "execution_plan": {
      "type": "object",
      "required": ["execution_order", "steps"],
      "properties": {
        "execution_order": { "type": "array", "items": { "type": "string" } },
        "steps": {
          "type": "object",
          "additionalProperties": {
            "type": "object",
            "required": ["description", "type", "provenance", "rationale", "success_criteria", "details"],
            "properties": {
              "description": { "type": "string" },
              "type": { "type": "string", "enum": ["tool_call", "llm_reasoning", "human_intervention", "no_tool_available"] },
              "provenance": { "type": "object" },
              "rationale": { "type": "string" },
              "success_criteria": {
                "type": "object",
                "required": ["evaluation_logic", "conditions"],
                "properties": {
                  "evaluation_logic": { "type": "string", "enum": ["ALL", "ANY"] },
                  "conditions": {
                    "type": "array",
                    "items": {
                      "oneOf": [
                        { "type": "http_status_code", "...": "..." },
                        { "type": "json_path_check", "...": "..." },
                        { "type": "string_contains", "...": "..." },
                        { "type": "regex_match", "...": "..." },
                        { "type": "semantic_check", "...": "..." }
                      ]
                    }
                  }
                }
              },
              "details": { "type": "object" }
            }
          }
        }
      }
    }
  }
}
			""";
			
			
			
			
			//TODO: We should re-add this component, with descriptions and reminders, to the above.
			//not just the raw json schema. We need to remake it based on the new schema.
			private static final String oldCotComponent = """

			{
			  "plan_id": "string (A unique identifier for the plan)",
			  "user_prompt": "string (The original prompt from the user)",
			  "steps": [
			    {
			      "step_number": "integer (Sequential order of the step, e.g., 1)",
			      "description": "string (A human-readable summary of the step's goal)",
			      "type": "string (Enum: 'tool_call' or 'llm_action')",
			      "details": {
			        // if type is 'tool_call'
			        "tool_name": "string (The name of the tool to execute, e.g., 'run_shell_command')",
			        "parameters": "object (A key-value map of parameters for the tool)",
			        "rationale": "string (Why this specific tool is needed for this step)",
			        // if type is 'llm_action'
			        "action": "string (The internal action the LLM is taking, e.g., 'reasoning', 'summarization', 'response_generation')",
			        "input": "string (Description of the data the LLM is processing, e.g., 'Content from step 1')",
			        "output": "string (Description of the expected outcome)",
			        "rationale": "string (Why this internal step is necessary, e.g., 'To formulate a response to the user')"
			      },
			      "status": "string (Enum: 'pending', 'in_progress', 'completed', 'failed')",
			      "result": "object (The output or result of the step, optional)"
			    }
			  ]
			}

			      Remember:
			      - Each step must be type "tool_call" or "llm_action" with appropriate details.
			      - Use the extra context provided above when it is relevant.
			      - Steps should have incremental step_number and clear rationales.
			      - Respond ONLY with the JSON object.
			      """;

	public static final String COT_JSON_SCHEMA = """
			{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Unified Agent Plan",
  "description": "A complete, step-by-step execution plan with provenance, success criteria, and multi-actor steps.",
  "type": "object",
  "required": ["prompt_context", "execution_plan"],
  "properties": {
    "prompt_context": { "...": "..." },
    "execution_plan": {
      "type": "object",
      "required": ["execution_order", "steps"],
      "properties": {
        "execution_order": { "type": "array", "items": { "type": "string" } },
        "steps": {
          "type": "object",
          "additionalProperties": {
            "type": "object",
            "required": ["description", "type", "provenance", "rationale", "success_criteria", "details"],
            "properties": {
              "description": { "type": "string" },
              "type": { "type": "string", "enum": ["tool_call", "llm_reasoning", "human_intervention", "no_tool_available"] },
              "provenance": { "type": "object" },
              "rationale": { "type": "string" },
              "success_criteria": {
                "type": "object",
                "required": ["evaluation_logic", "conditions"],
                "properties": {
                  "evaluation_logic": { "type": "string", "enum": ["ALL", "ANY"] },
                  "conditions": {
                    "type": "array",
                    "items": {
                      "oneOf": [
                        { "type": "http_status_code", "...": "..." },
                        { "type": "json_path_check", "...": "..." },
                        { "type": "string_contains", "...": "..." },
                        { "type": "regex_match", "...": "..." },
                        { "type": "semantic_check", "...": "..." }
                      ]
                    }
                  }
                }
              },
              "details": { "type": "object" }
            }
          }
        }
      }
    }
  }
}
			""";
	
	public static final String TOOL_ARGUMENTS_PROMPT = """
			Predict best arguments for the tool "%s" to accomplish the task described in this step.
			Tool info: %s
			%s
			%s
			Respond with a tool call for the above tool containing the tool arguments. You must call a tool.
			""";

}