/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.playground;

public class PlaygroundUtils {
	public static final String PLAYGROUND_PROJECT_ID = "SYSTEM__PLAYGROUND";
	public static final String PLAYGROUND_MESSAGE_TYPE = "PLAYGROUND_MESSAGE_TYPE";
	public static final String ENRICH_PROMPT = """
			# ROLE & GOAL

			You are an expert Requirements Analyst. Your purpose is to analyze an ambiguous user request. You can either generate a concise set of clarifying questions to gather necessary information, or, if the request allows, you can make reasonable assumptions, state them, and answer the prompt directly. Your goal is to be as helpful as possible, avoiding unnecessary questions for simple tasks while ensuring accuracy for complex ones.

			# TASK

			1.  **Analyze the User's Prompt:** Read the prompt and identify all implicit assumptions, missing variables, and areas of ambiguity.
			2.  **Decide Path:**
			    *   **If the prompt is simple and you can make a reasonable, low-risk assumption:** Proceed to the "Assumptions" path.
			    *   **If the prompt is complex, ambiguous, or requires user input:** Proceed to the "Clarification" path.
			3.  **Path 1: Clarification:**
			    *   Formulate a list of clear, targeted questions to ask the user.
			    *   Prioritize the questions logically.
			    *   Generate a JSON object with the `clarifying_questions` array populated.
			4.  **Path 2: Assumptions:**
			    *   Clearly state the assumption(s) you are making.
			    *   Provide a direct answer to the user's prompt based on your assumption(s).
			    *   Generate a JSON object with the `assumptions` array populated.
			5.  **Generate JSON:** Your output MUST be a single, valid JSON object that conforms to the schema below.

			""";

	public static final String ENRICH_SCHEMA = """
			{
			  "$schema": "http://json-schema.org/draft-07/schema#",
			  "title": "Clarification and Assumption Response",
			  "description": "A structured response that either asks clarifying questions or provides an answer based on stated assumptions.",
			  "type": "object",
			  "required": ["ambiguity_summary"],
			  "properties": {
			    "ambiguity_summary": {
			      "type": "string",
			      "description": "A brief, one-sentence explanation of why the original prompt is incomplete or a summary of the action taken."
			    },
			    "clarifying_questions": {
			      "type": "array",
			      "description": "An ordered list of questions to ask the user. This should be omitted if making assumptions.",
			      "items": {
			        "type": "object",
			        "required": ["question_key", "question_text"],
			        "properties": {
			          "question_key": { "type": "string" },
			          "question_text": { "type": "string" }
			        }
			      }
			    },
			    "assumptions": {
			      "type": "array",
			      "description": "A list of assumptions made to answer the prompt directly. This should be omitted if asking clarifying questions.",
			      "items": {
			        "type": "object",
			        "required": ["assumption_made", "answer_provided"],
			        "properties": {
			          "assumption_made": { "type": "string", "description": "The assumption made by the AI." },
			          "answer_provided": { "type": "string", "description": "The direct answer to the user's prompt based on the assumption." }
			        }
			      }
			    }
			  },
			  "oneOf": [
			    { "required": ["clarifying_questions"] },
			    { "required": ["assumptions"] }
			  ]
			}
			""";

	public static final String TRIAGE_PROMPT = """
			# ROLE & GOAL
			You are a hyper-efficient query classification agent. Your only job is to analyze the user's prompt and classify it into one of three categories based on the definitions provided. You do not answer the prompt.

			# CATEGORY DEFINITIONS
			1.  `direct_response`: Choose this if the prompt can be answered directly from a powerful LLM's general knowledge without needing external tools or specific reasoning on provided data.
			    *   *Examples: "What is 2+2?", "Write a poem about the sea."*

			2.  `single_step_execution`: Choose this if the prompt can be fully resolved by performing a single, specific action. This could be one tool call OR one reasoning task (like summarizing or translating the provided text).
			    *   *Examples: "What's the weather in Paris?", "Summarize this text: [text...]", "Who is the CEO of Google?"*

			3.  `complex_plan`: Choose this if the prompt requires multiple dependent steps, combining information from different sources, or orchestrating a workflow to resolve.
			    *   *Examples: "Onboard our new hire.", "Compare last quarter's sales to our marketing spend and create a report."*
			""";

	public static final String TRIAGE_SCHEMA = """
						{
			  "$schema": "http://json-schema.org/draft-07/schema#",
			  "title": "Triage Classification Output",
			  "description": "Defines the structured output for the initial prompt triage agent.",
			  "type": "object",
			  "required": [ "classification", "reasoning" ],
			  "properties": {
			    "classification": {
			      "type": "string",
			      "description": "The classification of the user's prompt.",
			      "enum": [ "direct_response", "single_step_execution", "complex_plan" ]
			    },
			    "reasoning": {
			      "type": "string",
			      "description": "A brief justification for the chosen classification."
			    }
			  }
			}
						""";

	public static final String UNGROUNDED_PLAN_PROMPT = """
						# ROLE & GOAL
			You are an expert system architect and planning agent. Your
			primary function is to analyze a user's request and generate
			a detailed, structured JSON object that outlines a comprehensive
			plan for fulfilling the request.

			# TASK
			1.  **Analyze the Prompt:** Understand the user's core intent and summarize the requirements.
			2.  **Identify Potential Resources:** Catalog all potential resources that might be needed. This includes databases, APIs, internal policies, and applications/automations.
			3.  **Create an Execution Plan:** Formulate a step-by-step "Chain of Thought" plan.
			4.  **Generate JSON:** Output the entire analysis, resource catalog, and plan as a single, valid JSON object.
			""";

	public static final String UNGROUNDED_PLAN_SCHEMA = """
						{
			  "prompt_analysis": {
			    "user_prompt": "string",
			    "llm_summary": "string",
			    "identified_intent": "string"
			  },
			  "identified_resources": {
			    "databases": [ { "database_name": "string", "table_name": "string", "rationale": "string" } ],
			    "policies": [ { "policy_name": "string", "description": "string", "rationale": "string" } ],
			    "apis": [ { "api_name": "string", "endpoint_suggestion": "string", "rationale": "string" } ]
			  },
			  "execution_plan": {
			    "execution_order": ["step_id_as_key"],
			    "steps": {
			      "step_id_as_key": {
			        "description": "string",
			        "type": "string (Enum: 'tool_call', 'code_generation', 'llm_knowledge', 'llm_reasoning', 'human_intervention')",
			        "provenance": { "source": "llm", "status": "original" },
			        "details": {
			            "required_role": ["string"],
			            "instructions": "string",
			            "expected_input": "string"
			         }
			      }
			    }
			  }
			}
			""";

	public static final String RAG_PRIORITIZATION_PROMPT = """
						# ROLE & GOAL
			You are an expert data source router. Your primary function is to analyze a
			user's query and, from a list of available data sources, select the most
			appropriate source(s) to answer the query.

			# AVAILABLE DATA SOURCES
			%s
			""";

	public static final String RAG_PRIORITIZATION_SCHEMA = """
			{
			  "$schema": "http://json-schema.org/draft-07/schema#",
			  "title": "LLM Router Output",
			  "type": "object",
			  "required": [ "reasoning", "selected_sources" ],
			  "properties": {
			    "reasoning": { "type": "string" },
			    "selected_sources": {
			      "type": "array",
			      "items": {
			        "type": "object",
			        "required": [ "priority", "source_name", "rationale", "optimized_query" ],
			        "properties": {
			          "priority": { "type": "integer" },
			          "source_name": { "type": "string" },
			          "rationale": { "type": "string" },
			          "optimized_query": { "type": "string" }
			        }
			      }
			    }
			  }
			}
			""";

	public static final String COT_SYSTEM_PROMPT_OLD = "You are an expert reasoning assistant that breaks down user queries into sequential steps using available tools and retrieved knowledge context, always formatting your output as valid JSON according to the provided schema.";

	public static final String COT_SYSTEM_PROMPT = """
			You are an AI assistant that follows a pre-generated, step-by-step JSON plan (chain of thought) to achieve a user's goals. You MUST NOT rewrite, regenerate, or update this JSON plan, unless specifically instructed to do so. Treat the provided plan strictly as a read-only, unchanging reference for your actions.

			For each interaction after plan creation:
			- DO NOT echo or output the plan JSON.
			- DO NOT modify, rephrase, reprioritize, or replan the steps.
			- DO NOT consolidate new information into the JSON plan.
			- DO NOT produce a new JSON plan in response to tool results.

			Instead, do the following:
			- Acknowledge the result/output of the last tool call, in natural language, focusing only on what is relevant for the next step.
			- Use this tool result (and other available information) to move on to the next step in the plan, executing or preparing the next required action as described in the plan.
			- If the next step requires user input or clarification, ask for it.
			- If the next step is a tool call, state what you are about to do, referencing the relevant information (including tool outputs as needed).
			- Continue sequentially through the plan, without skipping or changing steps or structure.

			Examples:
			- If you called a tool to get a list of fridge items and received "[eggs, milk, spinach]", respond like: "I see your fridge contains eggs, milk, and spinach. Next, I'll check your pantry items."
			- For a reasoning step: "Based on the ingredients available, I will now look for healthy recipes you can prepare."

			Never output or rewrite the plan JSON during step execution. Move forward naturally, narrating the process, until all plan steps are complete.

			Summarize or suggest only as required by the next plan step, always referencing only the current results and plan, not the entire plan.

			""";

	public static final String COT_PROMPT_TEMPLATE = """
			     You are an Expert AI Planning Agent. Your primary function is to create a
			     comprehensive, step-by-step execution plan. in JSON.


			     # TASK
			     1.  **Analyze Inputs:** Review the user prompt and the enriched context.
			     2.  **Formulate a Plan:** Create a step-by-step plan to fulfill the request.
			     3.  **Assign Actors:** For each step, determine if it should be `tool_call`, `llm_reasoning`, or `human_intervention`.
			     4.  **Identify Gaps:** If a necessary action cannot be performed, create a `no_tool_available` step.
			     5.  **Define Success:** For each step, you MUST define a machine-readable `success_criteria` object.
			     6.  **Describe Missing Tools:** For every `no_tool_available` step, set `details.missing_capability` to a short searchable phrase that describes the missing capability.

			     Available tools:
			     %s

			     Extra context (from knowledge base):
			     ```
			     %s
			     ```

			     User query:
			     %s
			""";
//
//	public static final String COT_JSON_SCHEMA = """
//						{
//			  "$schema": "http://json-schema.org/draft-07/schema#",
//			  "title": "Unified Agent Plan",
//			  "description": "A complete, step-by-step execution plan with provenance, success criteria, and multi-actor steps.",
//			  "type": "object",
//			  "required": ["prompt_context", "execution_plan"],
//			  "properties": {
//			    "prompt_context": { "...": "..." },
//			    "execution_plan": {
//			      "type": "object",
//			      "required": ["execution_order", "steps"],
//			      "properties": {
//			        "execution_order": { "type": "array", "items": { "type": "string" } },
//			        "steps": {
//			          "type": "object",
//			          "additionalProperties": {
//			            "type": "object",
//			            "required": ["description", "type", "provenance", "rationale", "success_criteria", "details"],
//			            "properties": {
//			              "description": { "type": "string" },
//			              "type": { "type": "string", "enum": ["tool_call", "llm_reasoning", "human_intervention", "no_tool_available"] },
//			              "provenance": { "type": "object" },
//			              "rationale": { "type": "string" },
//			              "success_criteria": {
//			                "type": "object",
//			                "required": ["evaluation_logic", "conditions"],
//			                "properties": {
//			                  "evaluation_logic": { "type": "string", "enum": ["ALL", "ANY"] },
//			                  "conditions": {
//			                    "type": "array",
//			                    "items": {
//			                      "oneOf": [
//			                        { "type": "http_status_code", "...": "..." },
//			                        { "type": "json_path_check", "...": "..." },
//			                        { "type": "string_contains", "...": "..." },
//			                        { "type": "regex_match", "...": "..." },
//			                        { "type": "semantic_check", "...": "..." }
//			                      ]
//			                    }
//			                  }
//			                }
//			              },
//			              "details": { "type": "object" }
//			            }
//			          }
//			        }
//			      }
//			    }
//			  }
//			}
//						""";
//

	public static final String COT_JSON_SCHEMA_NO_HUMAN_INTERVENTION = """
			{
			  "type": "object",
			  "properties": {
			    "user_prompt": { "type": "string" },
			    "steps": {
			      "type": "array",
			      "items": {
			        "type": "object",
			        "properties": {
			          "step_name": { "type": "string" },
			          "step_number": { "type": "integer" },
			          "description": { "type": "string" },
			          "type": {
			            "type": "string",
			            "enum": [
			              "tool_call",
			              "llm_reasoning",
			              "human_intervention",
			              "no_tool_available"
			            ]
			          },
			          "details": {
			            "anyOf": [
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "tool_name",
			                  "parameters",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": { "type": "string", "enum": ["tool_call"] },
			                  "tool_name": { "type": "string", "enum": [%s]  },
			                  "parameters": {
			                    "type": "object",
			                    "additionalProperties": true
			                  },
			                  "rationaleForStep": { "type": "string" }
			                }
			              },
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "prompt",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": { "type": "string", "enum": ["llm_reasoning"] },
			                  "prompt": { "type": "string" },
			                  "rationaleForStep": { "type": "string" }
			                }
			              },
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "missing_capability",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": {
			                    "type": "string",
			                    "enum": ["no_tool_available"]
			                  },
			                  "missing_capability": { "type": "string" },
			                  "rationaleForStep": { "type": "string" }
			                }
			              }
			            ]
			          },
			          "status": {
			            "type": "string",
			            "enum": ["pending", "in_progress", "completed", "failed"]
			          },
			          "result": {
			            "type": "object",
			            "nullable": true,
			            "additionalProperties": true
			          }
			        },
			        "required": ["step_name", "step_number", "description", "type", "details", "status"]
			      }
			    }
			  },
			  "required": ["user_prompt", "steps"]
			}
			""";

	public static final String COT_JSON_SCHEMA = """
			{
			  "type": "object",
			  "properties": {
			    "user_prompt": { "type": "string" },
			    "steps": {
			      "type": "array",
			      "items": {
			        "type": "object",
			        "properties": {
			          "step_name": { "type": "string" },
			          "step_number": { "type": "integer" },
			          "description": { "type": "string" },
			          "type": {
			            "type": "string",
			            "enum": [
			              "tool_call",
			              "llm_reasoning",
			              "human_intervention",
			              "no_tool_available"
			            ]
			          },
			          "details": {
			            "anyOf": [
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "tool_name",
			                  "parameters",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": { "type": "string", "enum": ["tool_call"] },
			                  "tool_name": { "type": "string", "enum": [%s]  },
			                  "parameters": {
			                    "type": "object",
			                    "additionalProperties": true
			                  },
			                  "rationaleForStep": { "type": "string" }
			                }
			              },
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "prompt",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": { "type": "string", "enum": ["llm_reasoning"] },
			                  "prompt": { "type": "string" },
			                  "rationaleForStep": { "type": "string" }
			                }
			              },
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "required_role",
			                  "instructions",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": {
			                    "type": "string",
			                    "enum": ["human_intervention"]
			                  },
			                  "required_role": { "type": "string" },
			                  "instructions": { "type": "string" },
			                  "rationaleForStep": { "type": "string" }
			                }
			              },
			              {
			                "type": "object",
			                "required": [
			                  "stepType",
			                  "missing_capability",
			                  "rationaleForStep"
			                ],
			                "properties": {
			                  "stepType": {
			                    "type": "string",
			                    "enum": ["no_tool_available"]
			                  },
			                  "missing_capability": { "type": "string" },
			                  "rationaleForStep": { "type": "string" }
			                }
			              }
			            ]
			          },
			          "status": {
			            "type": "string",
			            "enum": ["pending", "in_progress", "completed", "failed"]
			          },
			          "result": {
			            "type": "object",
			            "nullable": true,
			            "additionalProperties": true
			          }
			        },
			        "required": ["step_name", "step_number", "description", "type", "details", "status"]
			      }
			    }
			  },
			  "required": ["user_prompt", "steps"]
			}
			""";

	public static final String TOOL_ARGUMENTS_PROMPT = """
			Predict best arguments for the tool "%s" to accomplish the task described in this step.
			Tool info: %s
			%s
			%s
			Respond with a tool call for the above tool containing the tool arguments. You must call a tool.
			""";

	public static final String TOOL_ARGUMENTS_PREDICTION_PROMPT = """
			Predict best arguments for the tool "%s" to accomplish the task described in this step.
			%s
			Respond with a tool call for the above tool containing the tool arguments. You must call a tool.
			""";

	public static final String PLAN_VALIDATOR_PROMPT = """
			# ROLE & GOAL
			You are a hyper-efficient AI Plan Validator. Your sole purpose is to determine if a given
			plan is still the most logical and efficient path to a goal, based on new information you
			have just learned. You only make one decision: **continue** or **regenerate**.
			""";

	public static final String PLAN_VALIDATOR_SCHEMA = """
			{
			  "$schema": "http://json-schema.org/draft-07/schema#",
			  "title": "Plan Validator Decision",
			  "oneOf": [
			    {
			      "title": "Continue Decision",
			      "properties": { "decision": { "const": "continue" }, "reasoning": { "type": "string" } }
			    },
			    {
			      "title": "Regenerate Decision",
			      "properties": { "decision": { "const": "regenerate" }, "reasoning": { "type": "string" } }
			    }
			  ]
			}
			""";

	public static final String CONFIRM_COT_PLAN = """
			The following chain-of-thought plan for the user's request has been reviewed and confirmed.
			Please acknowledge the plan and begin executing steps sequentially, following the plan exactly as given.
			Do not modify or regenerate the plan.

			Confirmed Chain-of-Thought Plan (in JSON):
			%s
			""";

	public static final String FOLLOW_UP_SUGGESTIONS_PROMPT = """
			You generate follow-up suggestions only.
			Based on the most recent conversation, propose short, user-style follow-up questions that advance the same topic.
			Do not answer the user. Do not include preamble or numbering.
			Each suggestion must be a single sentence (<= 12 words).
			Avoid repeating the last user message verbatim.
			""";

	public static final String FOLLOW_UP_SUGGESTIONS_SCHEMA = """
			{
			  "title": "FollowUpSuggestions",
			  "type": "object",
			  "additionalProperties": false,
			  "required": ["suggestions"],
			  "properties": {
			    "suggestions": {
			      "type": "array",
			      "minItems": %s,
			      "maxItems": %s,
			      "items": { "type": "string" }
			    }
			  }
			}
			""";

}
