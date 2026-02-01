import os
import time
from datetime import date
import logging
from dotenv import load_dotenv
from collections import deque
from google import genai
from google.genai import types
from api.models import NLQRequest, NLQResult, MetricsIntent

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# -----------------------------------------------------------------
# Demo rate guard (in-memory)
# - RPM: rolling window limit (e.g. 5 requests / 60s)
# - RPD: daily limit (e.g. 20 requests / day)
# Notes:
# - In-memory only (resets on process restart)
# - Not shared across multiple workers/instances
# -----------------------------------------------------------------
RPM_MAX_CALLS = int(os.getenv("NLQ_RPM_MAX_CALLS", "5"))
RPM_WINDOW_SEC = int(os.getenv("NLQ_RPM_WINDOW_SEC", "60"))

RPD_MAX_CALLS = int(os.getenv("NLQ_RPD_MAX_CALLS", "20"))
_daily_date = date.today()
_daily_count = 0

_recent_calls = deque()


def _rate_guard() -> bool:
    """Return True if under demo quota; False if rate-limited."""
    global _daily_date, _daily_count

    now = time.time()

    # ---- RPD guard (reset on date change) ----
    today = date.today()
    if today != _daily_date:
        _daily_date = today
        _daily_count = 0

    if _daily_count >= RPD_MAX_CALLS:
        return False

    # ---- RPM guard (rolling window) ----
    while _recent_calls and now - _recent_calls[0] > RPM_WINDOW_SEC:
        _recent_calls.popleft()

    if len(_recent_calls) >= RPM_MAX_CALLS:
        return False

    # Consume quota on success path
    _recent_calls.append(now)
    _daily_count += 1
    return True

def _build_config() -> tuple[list[str], str, dict]:
    allowed_intents = [e.value for e in MetricsIntent]

    system_instruction = (
        "You are a strict intent classifier for an analytics API. "
        "Return ONLY a JSON object that matches the provided schema. "
        f"Allowed intents: {allowed_intents}. "
        "Choose the best matching intent. "
        "If the user query is NOT about alumni analytics metrics (companies, job titles, majors), "
        "set intent to 'unsupported'. "
        "limit must be an integer between 1 and 100. "
        "If the user does not specify a limit, use 10."
    )

    schema = {
        "type": "object",
        "properties": {
            "intent": {"type": "string", "enum": allowed_intents},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["intent", "limit"],
        "additionalProperties": False,
    }

    return allowed_intents, system_instruction, schema


def classify_intent(req: NLQRequest) -> NLQResult:
    """
    Classify NL query into a bounded contract (intent, limit) using Gemini.

    Reliability behavior:
    - 1 retry on transient errors (429/timeout-ish).
    - Fail closed: return intent=unsupported on repeated failure.
    """

    # Check Gemini RPM
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set; returning unsupported.")
        return NLQResult(intent=MetricsIntent.unsupported, limit=10)
    
    if not _rate_guard():
        logger.warning("NLQ rate limit exceeded (demo guard)")
        return NLQResult(intent=MetricsIntent.unsupported, limit=10)


    if not api_key:
        # Fail closed rather than pretending we classified
        logger.warning("GEMINI_API_KEY not set; returning unsupported.")
        return NLQResult(intent=MetricsIntent.unsupported, limit=10)

    allowed_intents, system_instruction, schema = _build_config()

    client = genai.Client(api_key=api_key)

    def _call_once() -> NLQResult:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=req.query,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                response_json_schema=schema,
            ),
        )

        data = response.parsed

        # Defensive casting even though schema already constrained it
        intent_str = str(data["intent"])
        limit = int(data["limit"])

        if intent_str not in allowed_intents:
            # Extremely unlikely due to schema, but keep it fail-closed
            logger.warning("Gemini returned unexpected intent '%s'; returning unsupported.", intent_str)
            return NLQResult(intent=MetricsIntent.unsupported, limit=10)

        return NLQResult(intent=MetricsIntent(intent_str), limit=limit)

    # First attempt
    try:
        return _call_once()
    except Exception as e:
        msg = str(e).lower()

        transient = (
            "429" in msg
            or "too many requests" in msg
            or "rate" in msg
            or "timeout" in msg
            or "timed out" in msg
            or "deadline" in msg
            or "temporarily" in msg
            or "unavailable" in msg
            or "503" in msg
        )

        # Log a short message (avoid dumping prompts or secrets)
        logger.warning("Gemini classify failed (transient=%s): %s", transient, type(e).__name__)

        if transient:
            # One small backoff then retry once
            time.sleep(0.4)
            try:
                return _call_once()
            except Exception as e2:
                logger.warning("Gemini classify retry failed: %s", type(e2).__name__)
                return NLQResult(intent=MetricsIntent.unsupported, limit=10)

        # Non-transient: fail closed immediately
        return NLQResult(intent=MetricsIntent.unsupported, limit=10)