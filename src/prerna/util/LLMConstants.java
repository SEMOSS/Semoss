package prerna.util;

public class LLMConstants {

	public static final String systemPromptLLMForCSV = """
			
	You are a schema-merging engine.
	You MUST decide whether input CSV schemas represent the same entity or different entities.
	
	RULE 1 (STRONG) — FORCE MERGE CONDITIONS
	
		You MUST merge two CSV schemas into one entity if ANY of these conditions are true:
		
		 1. Exact same primary key name
			(e.g., movie_id and movie_id)
		
		 2. Semantically similar primary key names
			(e.g., movie_id, film_id, cinema_id)
			You MUST treat these as the SAME ID.
		
		 3. They describe the SAME real-world entity
			(e.g., Movie, Film, Cinema Item → same entity)
			This overrides all other rules.
		
		 4. They share both an ID and a common attribute
			Example: Both have Title → strong indicator of same entity.
			MUST MERGE.
		 
		 5. They represent two parts of the same entity’s attributes
			Example:
			International_Gross
			Domestic_Gross
		    Both describe the same movie, so MUST MERGE.
	
	RULE 2 — WHEN TO SEPARATE
		
		Only separate tables if:
		
		IDs are completely unrelated AND
		
		They describe unrelated real-world objects AND
		
		There is no semantic relationship
		
		If both tables describe movies, you are NOT allowed to separate them.
		
	RULE 3 — FOREIGN KEY CHECK
		
		If an ID appears ONLY as a foreign key pointing to another table → do not merge.
		
		If an ID appears as a primary key for both schemas → merge.
		
	RULE 4 — EXTREMELY IMPORTANT (HARD OVERRIDE)
		If two schemas describe the same real entity type (e.g., Movies), you MUST merge even if the model is unsure.
		
		Uncertainty is NOT allowed.
		When in doubt → MERGE.
		
	RULE 5 — EXAMPLE TO ENFORCE MERGE
		
		If the user gives these schemas:
		
		Schema1:
		Movie_ID (INT)
		Title (STRING)
		International_Gross (INT)
		
		Schema2:
		Movie_ID (INT)
		Title (STRING)
		Domestic_Gross (INT)
		
		
		You MUST output ONE MERGED ENTITY named Movie or Movies.
		
		Never output separate entities for this scenario.
		Never create relationships.
		Never treat Movie_ID as a foreign key.
		
	RULE 6 — OUTPUT STRUCTURE
		
		Always output:
		
		{
		  "tables": [...],
		  "relationships": [...],
		  "nodeProp": {...}
		}
		
		FINAL HARD RULE (CANNOT BE VIOLATED)
		
		If two schemas share:
		
		the same PK name, OR
		
		a semantically similar PK, OR
		
		common business attributes (title, name), OR
		
		describe the same real-world concept
		
		→ YOU MUST MERGE.
		Separating is forbidden.
		
	RULE 7 — What is nodeProp?
		
		nodeProp is a JSON object where:
		
		Each table name becomes a node type (key).
		
		Each node contains a list of properties derived from the columns of that table.
		
		Example format:
		"nodeProp": {
		   "Movie": ["Movie_ID", "Title", "Domestic_Gross", "International_Gross"]
		}
		
	RULE 8 — nodeProp MUST match the final tables
		
		You MUST generate nodeProp only after the tables are finalized (merged or separated).
		
		If tables are merged → one nodeProp entry
		
		If tables are separated → multiple nodeProp blocks
		
		nodeProp must always be 1-to-1 mapped with "tables" list
		
	RULE 9 — Column order rules
		
		Inside each node:
		
		Primary key MUST come first
		
		Sorted order for remaining columns:
		
		Descriptive attributes (title, name, code)
		
		Measurable numeric attributes (values, gross)
		
		Timestamp columns last (created_at, updated_at)
		
		RULE 10 — Merged entity nodeProp rules
		
		If schemas are merged:
		
		You MUST:
		
		Combine all columns across schemas
		
		Remove duplicates
		
		Maintain PK first
		
		Use the chosen merged table name as the nodeProp key
		
		Never create separate nodeProps if tables are merged
		
	RULE 11 — Separate entity nodeProp rules
		
		If tables are separate:
		
		You MUST:
		
		Create separate nodeProp entries for each table
		
		Include only columns from that table
		
		Do NOT create combined nodeProps
		
	RULE 12 — No additional attributes
		
		Never infer or generate new attributes.
		Only use attributes present in the input schemas.
		
		Final Output Format
		
		You MUST output nodeProp in this format:
		
		"nodeProp": {
		   "<TableName1>": ["col1", "col2", ...],
		   "<TableName2>": ["col1", "col2", ...]
		}
		
		
		This appears after "tables" and "relationships".
		
	RULE 12 — Example (ENFORCED)
		
		For this input:
		
		Schema1: Movie_ID, Title, International_Gross
		Schema2: Movie_ID, Title, Domestic_Gross
		
		
		Merged table name example: Movie
		
		nodeProp MUST be:
		"nodeProp": {
		   "Movie": ["Movie_ID", "Title", "International_Gross", "Domestic_Gross"]
		}
		
		
		Never generate separated nodeProp for this example.
			""";
	
	public static final String outputSchemaCSV = """
			{
			  "schema": {
			    "type": "object",
			    "description": "Represents the fully structured output generated after analyzing one or more tabular schema inputs. The response describes inferred tables, their attributes, node-level mappings, and structural relationships derived entirely from column patterns, datatype similarity, and identifier alignment.",
			    "properties": {

			      "tables": {
			        "type": "array",
			        "description": "A collection of all inferred tables derived from the input schemas. Each entry describes one normalized table, including its name, its columns with associated datatypes, and the originating data source name without referencing file extensions.",
			        "items": {
			          "type": "object",
			          "description": "Defines a single inferred table. A table may correspond to one input schema or to a merged representation of multiple schemas that share identifiers or structural patterns.",
			          "properties": {
			            "tableName": {
			              "type": "string",
			              "description": "Logical name assigned to the inferred table. This name does not include any file extensions and represents the entity or structure consolidated from the provided schema."
			            },
			            "dataTypes": {
			              "type": "array",
			              "description": "A list of column entries for this table. Each entry specifies a column label and its associated inferred datatype. The model does not hallucinate column names; it only outputs values derived from the input.",
			              "items": {
			                "type": "object",
			                "description": "Defines a single column and its datatype within a table.",
			                "properties": {
			                  "column": {
			                    "type": "string",
			                    "description": "The exact name of the column as observed in the input schema. The value must remain unmodified to ensure structural integrity."
			                  },
			                  "type": {
			                    "type": "string",
			                    "description": "Datatype inferred from the provided schema input. The value must be exactly the datatype specified in the user input."
			                  }
			                },
			                "required": ["column", "type"]
			              }
			            },
			            "fileName": {
			              "type": "string",
			              "description": "Name assigned to the data source from which this table was derived. This name must not include any file extensions and should reflect only the core source identifier."
			            }
			          },
			          "required": ["tableName", "dataTypes", "fileName"]
			        }
			      },

			      "nodeProp": {
			        "type": "array",
			        "description": "Defines the mapping between each inferred table and the list of columns belonging to it. This structure is returned as an array to maintain compatibility with JSON Schema constraints that prevent dynamic key objects without additionalProperties.",
			        "items": {
			          "type": "object",
			          "description": "Describes one table-to-column mapping entry.",
			          "properties": {
			            "tableName": {
			              "type": "string",
			              "description": "Name of the table whose column list is defined in this mapping. Must match one of the table names declared in the 'tables' array."
			            },
			            "columns": {
			              "type": "array",
			              "description": "List of column names belonging to the table. Each column must appear exactly once and must correspond to a column in the associated 'dataTypes' list.",
			              "items": {
			                "type": "string",
			                "description": "A column name belonging to the table."
			              }
			            }
			          },
			          "required": ["tableName", "columns"]
			        }
			      },

			      "relation": {
			        "type": "array",
			        "description": "List of all inferred relationships among the tables. Each relationship expresses logical associations derived from structural identifiers, shared columns, or parent–child patterns detected in the schema.",
			        "items": {
			          "type": "object",
			          "description": "Represents one inferred relationship between two tables, including cardinality and column-level connections.",
			          "properties": {
			            "relName": {
			              "type": "string",
			              "description": "Deterministic relationship label. Typically a lower_snake_case combination of the connected tables with a suffix indicating association."
			            },
			            "linkedColumns": {
			              "type": "array",
			              "description": "Fully qualified column references indicating which fields form the link. Format: '<tableName.columnName>'.",
			              "items": {
			                "type": "string",
			                "description": "One side of a column-level link between tables."
			              }
			            },
			            "fromTable": {
			              "type": "string",
			              "description": "Name of the referencing table — the source of the relationship."
			            },
			            "toTable": {
			              "type": "string",
			              "description": "Name of the referenced table — the target of the relationship."
			            },
			            "type": {
			              "type": "string",
			              "enum": ["ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_MANY"],
			              "description": "Relationship cardinality inferred from identifier patterns, column uniqueness, or structural alignment."
			            },
			            "description": {
			              "type": "string",
			              "description": "Full explanation of why this relationship exists, referencing identifier similarity or structural linkage without domain-specific terminology."
			            }
			          },
			          "required": [
			            "relName",
			            "linkedColumns",
			            "fromTable",
			            "toTable",
			            "type",
			            "description"
			          ]
			        }
			      }
			    },

			    "required": ["tables", "nodeProp", "relation"]
			  }
			}


						""";




}
