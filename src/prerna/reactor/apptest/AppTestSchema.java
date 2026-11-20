package prerna.reactor.apptest;

import java.util.List;
import org.json.JSONObject;

/** Single contract for the generated reactor tool and the shipped platform toolbox. */
final class AppTestSchema {
    static final List<String> TYPES = List.of("CLICK", "FILL", "SELECT", "CHECK", "KEY", "SCROLL", "ASSERT_TEXT", "ASSERT_VISIBLE", "NAVIGATE", "ASSERT_VALUE", "ASSERT_CHECKED", "ASSERT_COUNT", "ASSERT_HIDDEN", "ASSERT_ABSENT", "ASSERT_URL", "WAIT_FOR", "DRAG");
    private static final String RECORDING = """
        {
          "type": "object",
          "description": "Declarative test object, never a JSON string. Omit id for new tests; use the saved UUID for updates. Paths are relative to portals/. pagePath is the expected route BEFORE a step; framePath contains strings for nested iframes, not row/lane containers. Optional cleanupSteps, per-attempt variables and explicit portal-root route matching are supported.",
          "required": [
            "name",
            "description",
            "steps"
          ],
          "properties": {
            "name": {
              "type": "string",
              "minLength": 1,
              "maxLength": 120
            },
            "description": {
              "type": "string",
              "minLength": 1,
              "maxLength": 2000,
              "description": "Explain the workflow and expected outcome this test verifies."
            },
            "steps": {
              "type": "array",
              "items": {
                "type": "object",
                "description": "Declarative action/assertion. Optional pagePath is the app-relative top-level URL expected before the step; replay waits for it, including query/hash routes.",
                "required": [
                  "type"
                ],
                "properties": {
                  "type": {
                    "type": "string",
                    "enum": [
                      "CLICK",
                      "FILL",
                      "SELECT",
                      "CHECK",
                      "KEY",
                      "SCROLL",
                      "ASSERT_TEXT",
                      "ASSERT_VISIBLE",
                      "NAVIGATE",
                      "ASSERT_VALUE",
                      "ASSERT_CHECKED",
                      "ASSERT_COUNT",
                      "ASSERT_HIDDEN",
                      "ASSERT_ABSENT",
                      "ASSERT_URL",
                      "WAIT_FOR",
                      "DRAG"
                    ]
                  },
                  "selector": {
                    "type": "string",
                    "description": "CSS selector required except for NAVIGATE and ASSERT_URL. Prefer a unique data-testid."
                  },
                  "path": {
                    "type": "string",
                    "description": "Required for NAVIGATE and ASSERT_URL; app-relative destination/expected route, never url. An explicit empty string means portal root."
                  },
                  "pagePath": {
                    "type": "string",
                    "description": "App-relative URL expected BEFORE this action, not its destination; preserve entry filename/query/hash."
                  },
                  "framePath": {
                    "type": "array",
                    "maxItems": 5,
                    "items": {
                      "type": "string"
                    },
                    "description": "Optional CSS selectors for nested iframes; omit or [] for the app document."
                  },
                  "value": {
                    "type": "string",
                    "description": "String required for FILL, SELECT or ASSERT_VALUE. SELECT/ASSERT_VALUE use the option/input value, not option labels."
                  },
                  "text": {
                    "type": "string",
                    "description": "Required for ASSERT_TEXT; checks element text content, not input/select value."
                  },
                  "exact": {
                    "type": "boolean"
                  },
                  "checked": {
                    "type": "boolean",
                    "description": "Required for CHECK and ASSERT_CHECKED."
                  },
                  "key": {
                    "type": "string",
                    "description": "Required for KEY."
                  },
                  "x": {
                    "type": "number",
                    "description": "Required for SCROLL."
                  },
                  "y": {
                    "type": "number",
                    "description": "Required for SCROLL."
                  },
                  "count": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 10000,
                    "description": "Expected match count for ASSERT_COUNT; ASSERT_ABSENT requires zero matches."
                  },
                  "state": {
                    "type": "string",
                    "enum": [
                      "visible",
                      "hidden",
                      "attached",
                      "detached"
                    ],
                    "description": "Required for WAIT_FOR; no fixed sleeps."
                  },
                  "timeoutMs": {
                    "type": "integer",
                    "minimum": 100,
                    "maximum": 30000,
                    "description": "Optional bounded step timeout, also limited by the remaining run deadline."
                  },
                  "targetSelector": {
                    "type": "string",
                    "description": "DRAG destination in the same framePath; selector is the source."
                  }
                },
                "anyOf": [
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "CLICK"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "FILL"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "value"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "SELECT"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "value"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "CHECK"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "checked"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "KEY"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "key"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "SCROLL"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "x",
                      "y"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_TEXT"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "text"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_VISIBLE"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "NAVIGATE"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "path"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_VALUE"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "value"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_CHECKED"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "checked"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_COUNT"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "count"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_HIDDEN"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_ABSENT"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "ASSERT_URL"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "path"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "WAIT_FOR"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "state"
                    ]
                  },
                  {
                    "properties": {
                      "type": {
                        "enum": [
                          "DRAG"
                        ]
                      }
                    },
                    "required": [
                      "type",
                      "selector",
                      "targetSelector"
                    ]
                  }
                ]
              },
              "minItems": 1,
              "maxItems": 300
            },
            "id": {
              "type": "string",
              "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
              "description": "Omit for a new test. For updates use its saved UUID, also used as its filename; never a test name or app id."
            },
            "startPath": {
              "type": "string",
              "description": "Relative to portals/, including entry filename/query/hash. Empty string means portal root. Example: index.html?env=test#/boards/b1. No leading slash."
            },
            "version": {
              "type": "integer",
              "enum": [
                1
              ]
            },
            "routeMatch": {
              "type": "string",
              "enum": [
                "EXACT",
                "PORTAL_ROOT"
              ],
              "description": "Default EXACT. PORTAL_ROOT explicitly treats portal directory/index.html and empty/#/#/ fragments as equivalent only at the portal entry. Queries always match exactly."
            },
            "useVariables": {
              "type": "boolean",
              "description": "Default false. Enable built-in {{runId}}, {{testId}}, {{attemptId}}, {{uniqueId}} placeholders. uniqueId is consistent within an attempt and differs across repeats."
            }
          }
        }
        """;

    private AppTestSchema() { }

    static JSONObject recording() {
        JSONObject value = new JSONObject(RECORDING);
        JSONObject cleanup = new JSONObject(value.getJSONObject("properties").getJSONObject("steps").toString());
        cleanup.put("minItems", 0);
        cleanup.put("maxItems", 50);
        value.getJSONObject("properties").put("cleanupSteps", cleanup);
        return value;
    }
}
