"""Smoke tests for the SEMOSS ↔ LangGraph adapter.

Runs offline: no model calls, no MCP servers, no pixel round-trips. Uses
LangChain's ``FakeListChatModel`` to construct a real ``CompiledGraph``
and asserts basic shape.

Run from ``Semoss_Dev/py``::

    py -3.12 -m unittest genai_client.tests.test_langgraph_agent
"""

from __future__ import annotations

import json
import unittest


class LangGraphAdapterTests(unittest.TestCase):
    def _fake_model(self):
        from langchain_community.chat_models.fake import FakeListChatModel

        return FakeListChatModel(responses=["ok"])

    def test_from_config_builds_react_graph(self):
        from genai_client.agents.langgraph_agent import (
            SemossAgent,
            SemossAgentConfig,
        )

        cfg = SemossAgentConfig(
            system_prompt="You are a helper.",
            model=self._fake_model(),
        )
        graph = SemossAgent.from_config(cfg)
        self.assertTrue(hasattr(graph, "invoke"))
        self.assertTrue(hasattr(graph, "stream"))

    def test_from_config_rejects_missing_model(self):
        from genai_client.agents.langgraph_agent import (
            SemossAgent,
            SemossAgentConfig,
        )

        with self.assertRaises(ValueError):
            SemossAgent.from_config(SemossAgentConfig(system_prompt="x"))

    def test_subagent_ref_accepts_camelcase_and_snake_case(self):
        from genai_client.agents.langgraph_agent import SubAgentRef

        camel = SubAgentRef.model_validate(
            {"alias": "researcher", "workspaceId": "abc", "description": "d"}
        )
        snake = SubAgentRef.model_validate(
            {"alias": "researcher", "workspace_id": "abc"}
        )
        self.assertEqual(camel.workspace_id, "abc")
        self.assertEqual(snake.workspace_id, "abc")

    def test_from_workspace_uses_pixel_loader(self):
        from genai_client.agents.langgraph_agent import SemossAgent

        model = self._fake_model()
        seen = []

        def loader(pixel: str):
            seen.append(pixel)
            return {
                "workspace_id": "root-ws",
                "name": "Root",
                "system_prompt": "prompt",
                "mcp": [],
                "config_json": json.dumps({"subagents": [], "mode": "react"}),
            }

        graph = SemossAgent.from_workspace(
            "root-ws", model=model, pixel_loader=loader
        )
        self.assertTrue(hasattr(graph, "invoke"))
        self.assertEqual(len(seen), 1)
        self.assertIn("root-ws", seen[0])

    def test_config_json_string_parses(self):
        from genai_client.agents.langgraph_agent.agent import (
            _config_from_workspace_output,
        )

        out = _config_from_workspace_output(
            {
                "workspace_id": "w",
                "config_json": json.dumps(
                    {
                        "subagents": [
                            {"alias": "r", "workspaceId": "child", "description": "d"}
                        ],
                        "mode": "deep",
                    }
                ),
            }
        )
        self.assertEqual(out["mode"], "deep")
        self.assertEqual(len(out["subagents"]), 1)


if __name__ == "__main__":
    unittest.main()
