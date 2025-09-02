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

			Produce your chain of thought steps as a JSON object.
			Follow the structure below, where each key is explained.

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
	
	public static final String TOOL_ARGUMENTS_PROMPT = """
			Predict best arguments for the tool "%s" to accomplish the task described in this step.
			Tool info: %s
			%s
			%s
			Respond with a tool call for the above tool containing the tool arguments. You must call a tool.
			""";

}