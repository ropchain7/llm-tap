# OpenAI-compatible logging proxy (llm-tap)

Small HTTP proxy for OpenAI-compatible APIs.

It accepts the
requests a client would send to an upstream API, forwards them to a configurable
upstream (eg OpenRouter), returns the response, and appends a
request and response records to a daily JSONL log.

SAF.
