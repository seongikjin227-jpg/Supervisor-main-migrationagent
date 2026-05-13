# SQL Prompt Debug Snippet

`server/services/sql/prompt_service.py`에서 실제 LLM에 들어가는 프롬프트를 파일로 확인하고 싶을 때만 임시로 복붙해서 사용합니다.

대상 프롬프트:

- `tobe_sql_prompt.json`
- `tobe_sql_tuning_prompt.json`
- `bind_sql_prompt.json`

## 사용 위치

`build_prompt_messages()` 함수를 아래 코드로 잠시 교체합니다.

```python
def build_prompt_messages(filename: str, **kwargs) -> list[dict[str, str]]:
    payload = render_prompt_template(filename, **kwargs)
    user_instruction = payload.pop("user_instruction", "Generate one executable Oracle SQL statement only.")
    messages = [
        {"role": "system", "content": _render_message_content(payload)},
        {"role": "user", "content": str(user_instruction)},
    ]

    debug_prompt_files = {
        "tobe_sql_prompt.json",
        "tobe_sql_tuning_prompt.json",
        "bind_sql_prompt.json",
    }
    if filename in debug_prompt_files:
        debug_dir = Path(__file__).resolve().parent / "debug_prompts"
        debug_dir.mkdir(exist_ok=True)

        stem = Path(filename).stem
        md_path = debug_dir / f"{stem}_debug.md"
        json_path = debug_dir / f"{stem}_debug_payload.json"

        md_path.write_text(
            "\n\n".join(
                [
                    f"# {filename}",
                    "## system",
                    messages[0]["content"],
                    "## user",
                    messages[1]["content"],
                ]
            ),
            encoding="utf-8",
        )

        json_path.write_text(
            json.dumps(
                {
                    "filename": filename,
                    "kwargs": kwargs,
                    "payload": payload,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    return messages
```

## 생성 위치

실행하면 아래 폴더에 파일이 생성됩니다.

```text
server/services/sql/debug_prompts/
```

주로 볼 파일:

```text
tobe_sql_prompt_debug.md
tobe_sql_tuning_prompt_debug.md
bind_sql_prompt_debug.md
```

참고용 원재료 파일:

```text
tobe_sql_prompt_debug_payload.json
tobe_sql_tuning_prompt_debug_payload.json
bind_sql_prompt_debug_payload.json
```

## 확인 기준

- `.md`: 실제 `messages[0].content`, `messages[1].content`를 사람이 읽는 형태로 확인합니다.
- `.json`: `kwargs`, `payload` 원재료를 확인합니다. JSON 파일 특성상 줄바꿈은 `\n`으로 보일 수 있습니다.

디버깅이 끝나면 반드시 원래 `build_prompt_messages()` 코드로 되돌립니다. SQL 원문과 매핑 정보가 파일에 남을 수 있습니다.
