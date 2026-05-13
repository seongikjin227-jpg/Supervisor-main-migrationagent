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
        {"role": "system", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
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
        debug_path = debug_dir / f"{Path(filename).stem}_debug.json"
        debug_path.write_text(
            json.dumps(
                {
                    "filename": filename,
                    "kwargs": kwargs,
                    "payload": payload,
                    "messages": messages,
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

생성 파일:

```text
tobe_sql_prompt_debug.json
tobe_sql_tuning_prompt_debug.json
bind_sql_prompt_debug.json
```

## 확인 포인트

- `kwargs`: 템플릿 치환 전 원재료 값
- `payload`: JSON 템플릿에 값이 치환된 결과
- `messages[0].content`: 실제 LLM system message
- `messages[1].content`: 실제 LLM user message

디버깅이 끝나면 반드시 원래 `build_prompt_messages()` 코드로 되돌립니다. SQL 원문과 매핑 정보가 파일에 남을 수 있습니다.
