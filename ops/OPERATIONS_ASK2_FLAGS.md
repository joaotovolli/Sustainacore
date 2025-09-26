# /ask2 Feature Flags & Defaults

The `/ask2` endpoint now relies on built-in defaults so it continues to operate even
when environment variables are missing. Unless explicitly overridden, the server
starts with the following configuration:

- `SMALL_TALK` — **On** (`true`). Enables the small-talk router so greetings such
  as "hi" or "thanks" are answered locally without calling retrieval.
- `INLINE_SOURCES` — **Off** (`false`). Keeps inline "Sources" footers out of the
  answer text. The UI renders source attributions only from the optional
  `contexts[]` array in the response body.
- `SIMILARITY_FLOOR_MODE` — **monitor**. The server computes similarity-floor
  decisions for logging and tracing but does not alter the returned payload unless
  the mode is set to `enforce`.
- `SIMILARITY_FLOOR` — **0.65** on the current 0–1 similarity scale. Requests with
  a top-1 score below this threshold trigger the below-floor decision path used by
  monitoring or enforcement.

These defaults guarantee that `/ask2` behaves safely even when the corresponding
environment variables are absent.
