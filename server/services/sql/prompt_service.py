import json
import re
from pathlib import Path
from typing import Any


# config/prompts/ (project_root 기준)
# agents/sql_pipeline/services/ → services → sql_pipeline → agents → root
PROMPTS_DIR = Path(__file__).resolve().parent.parent.parent / "config" / "prompts"


def load_prompt_template(filename: str) -> dict[str, Any]:
    prompt_path = PROMPTS_DIR / filename
    return json.loads(prompt_path.read_text(encoding="utf-8"))


def render_prompt_template(filename: str, **kwargs) -> dict[str, Any]:
    template = load_prompt_template(filename)
    return _render_value(template, kwargs)


def build_prompt_messages(filename: str, **kwargs) -> list[dict[str, str]]:
    payload = render_prompt_template(filename, **kwargs)
    user_instruction = payload.pop("user_instruction", "Generate one executable Oracle SQL statement only.")
    return [
        {"role": "system", "content": _render_message_content(payload)},
        {"role": "user", "content": str(user_instruction)},
    ]


def _render_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {key: _render_value(item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [_render_value(item, context) for item in value]
    if isinstance(value, str):
        return value.format(**context)
    return value


def _render_message_content(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    for key in ("name", "role", "objective"):
        value = payload.get(key)
        if value:
            lines.append(f"{key}: {value}")

    inputs = payload.get("inputs")
    if isinstance(inputs, dict) and inputs:
        lines.append("")
        lines.append("inputs:")
        for input_name, input_value in inputs.items():
            lines.extend(_render_input_block(input_name, input_value))

    rules = payload.get("rules")
    if isinstance(rules, list) and rules:
        lines.append("")
        lines.append("rules:")
        for rule in rules:
            lines.append(f"- {rule}")

    remaining = {
        key: value
        for key, value in payload.items()
        if key not in {"name", "role", "objective", "inputs", "rules"}
    }
    if remaining:
        lines.append("")
        lines.append("metadata:")
        lines.append(json.dumps(remaining, ensure_ascii=False, indent=2))

    return "\n".join(lines).strip()


def _render_input_block(name: str, value: Any) -> list[str]:
    text = _stringify_input_value(value)
    block_type = _detect_block_type(name, text)
    return [
        f"{name}:",
        f"```{block_type}",
        text,
        "```",
    ]


def _stringify_input_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def _detect_block_type(name: str, value: str) -> str:
    lowered_name = name.lower()
    if lowered_name.endswith("_json") or lowered_name in {
        "universal_tuning_rules",
        "searched_tuning_rule_block_rag_json",
    }:
        return "json"
    if "sql" in lowered_name or re.search(r"\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bMERGE\b", value, re.IGNORECASE):
        return "sql"
    return "text"
