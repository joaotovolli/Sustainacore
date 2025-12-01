from __future__ import annotations

import json
from typing import Any, Dict

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from . import client


def ask2_page(request: HttpRequest) -> HttpResponse:
    """Serve a minimal Ask2 page without templates."""
    html = """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <title>SustainaCore Ask2</title>
    </head>
    <body>
      <h1>SustainaCore Ask2</h1>
      <p>Type a question and we will forward it to the backend.</p>

      <textarea id="msg" rows="4" cols="80" placeholder="Type your question here..."></textarea>
      <br>
      <button id="sendBtn">Send</button>

      <h2>Reply</h2>
      <pre id="reply"></pre>

      <script>
      (function() {
          const btn = document.getElementById('sendBtn');
          const msg = document.getElementById('msg');
          const out = document.getElementById('reply');

          function setReply(text) {
              out.textContent = text;
          }

          btn.addEventListener('click', async function() {
              const text = (msg.value || '').trim();
              if (!text) {
                  setReply('Please enter a message.');
                  return;
              }
              setReply('Sending...');

              try {
                  const resp = await fetch('/ask2/api/', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ message: text })
                  });
                  const data = await resp.json();
                  if (!resp.ok) {
                      setReply(data.message || data.error || 'Request failed.');
                      return;
                  }
                  const reply = data.reply || data.content || data.message || 'No reply received.';
                  setReply(reply);
              } catch (err) {
                  setReply(err && err.message ? err.message : 'Failed to reach Ask2 API.');
              }
          });
      })();
      </script>
    </body>
    </html>
    """
    return HttpResponse(html)


@csrf_exempt
def ask2_api(request: HttpRequest) -> JsonResponse:
    """Proxy Ask2 chat requests to the VM1 backend."""
    if request.method != "POST":
        return JsonResponse({"error": "method_not_allowed", "message": "Use POST."}, status=405)

    user_message = ""
    if request.content_type and "application/json" in request.content_type:
        try:
            payload = json.loads(request.body or b"{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "invalid_payload", "message": "Invalid JSON payload."}, status=400)
        user_message = (payload.get("message") or payload.get("user_message") or "").strip()
    else:
        user_message = (
            request.POST.get("message")
            or request.POST.get("user_message")
            or ""
        ).strip()

    if not user_message:
        return JsonResponse({"error": "missing_message", "message": "Message is required."}, status=400)

    result = client.ask2_query(user_message)

    status_code = 200 if "error" not in result else 502
    response_data: Dict[str, Any] = {
        "session_id": result.get("session_id"),
        "reply": result.get("reply"),
        "answer": result.get("answer"),
        "content": result.get("content"),
        "message": result.get("message"),
    }
    if "error" in result:
        response_data["error"] = result.get("error")

    return JsonResponse(response_data, status=status_code)
