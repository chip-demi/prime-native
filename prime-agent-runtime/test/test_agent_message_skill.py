from __future__ import annotations

import asyncio
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


SKILL = Path(__file__).parents[2] / "skills/agent-message/src/agent_message/__init__.py"


class AgentMessageSkillTest(unittest.TestCase):
    def test_roled_parent_and_broadcast_forms(self) -> None:
        spec = importlib.util.spec_from_file_location("agent_message_test", SKILL)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        host = AsyncMock(return_value={"deliveryStatus": "queued"})
        with patch.object(module, "host_request", host), patch.object(module, "_emit_sent_message"):
            asyncio.run(module.send("done", receiver_role="parent"))
            asyncio.run(module.send("all", "follow up"))
        self.assertEqual(host.await_args_list[0].args[1]["receiver_role"], "parent")
        self.assertEqual(host.await_args_list[1].args[1]["target"], "all")

    def test_completion_opt_in_and_rejection_before_host(self) -> None:
        spec = importlib.util.spec_from_file_location("agent_message_completion", SKILL)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        host = AsyncMock(return_value={"deliveryStatus": "queued"})
        completion = {"assignment_id": "B", "artifact_ref": "B.md"}
        with patch.object(module, "host_request", host), patch.object(module, "_emit_sent_message"):
            asyncio.run(module.send("task B", receiver_role="sibling", receiver_name="worker", completion=completion))
            self.assertEqual(host.await_args.args[1]["completion"], completion)
            host.reset_mock()
            for kwargs in [
                {"receiver_role": "child", "receiver_name": "worker", "completion": completion},
                {"receiver_role": "sibling", "receiver_name": "worker", "completion": {"assignment_id": "B"}},
                {"receiver_role": "sibling", "receiver_name": "worker", "completion": dict(completion, creator="forged")},
            ]:
                with self.assertRaises(ValueError):
                    asyncio.run(module.send("task", **kwargs))
            with self.assertRaises(ValueError):
                asyncio.run(module.send("all", "task", completion=completion))
            host.assert_not_awaited()

    def test_roled_selector_validation(self) -> None:
        spec = importlib.util.spec_from_file_location("agent_message_validation", SKILL)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with self.assertRaisesRegex(ValueError, "required"):
            asyncio.run(module.send("hello", receiver_role="child"))
        with self.assertRaisesRegex(ValueError, "omitted"):
            asyncio.run(module.send("hello", receiver_role="parent", receiver_name="x"))
        with self.assertRaisesRegex(TypeError, "unexpected keyword argument 'mode'"):
            asyncio.run(module.send("hello", receiver_role="parent", mode="follow_up"))


if __name__ == "__main__":
    unittest.main()
