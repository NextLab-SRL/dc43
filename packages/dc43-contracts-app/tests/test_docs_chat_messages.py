from dc43_contracts_app import docs_chat


def test_coerce_chat_prompt_accepts_mapping():
    prompt = docs_chat._coerce_chat_prompt({"role": "user", "content": "hello"})  # type: ignore[attr-defined]

    assert prompt == "hello"


def test_coerce_chat_prompt_handles_sequence_payload():
    payload = {"role": "user", "content": [{"text": "part"}, " two"]}

    prompt = docs_chat._coerce_chat_prompt(payload)  # type: ignore[attr-defined]

    assert prompt == "part two"


def test_coerce_chat_history_pairs_messages():
    history = [
        {"role": "user", "content": "first question"},
        {"role": "assistant", "content": "first answer"},
        {"role": "user", "content": [{"text": "second"}, " question"]},
        {"role": "assistant", "content": {"text": "second answer"}},
    ]

    tuples = docs_chat._coerce_chat_history(history)  # type: ignore[attr-defined]

    assert tuples == [("first question", "first answer"), ("second question", "second answer")]


def test_coerce_chat_history_accepts_tuple_pairs():
    tuples = docs_chat._coerce_chat_history([("prompt", "response")])  # type: ignore[attr-defined]

    assert tuples == [("prompt", "response")]
