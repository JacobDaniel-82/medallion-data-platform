import json
import os
import sys
import urllib.request

WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")


def send(message):
    if not WEBHOOK_URL:
        print("ALERT (no webhook configured): " + message)
        return False

    # Slack expects {"text": ...}, Discord expects {"content": ...}.
    field = "text" if "slack.com" in WEBHOOK_URL else "content"
    body = json.dumps({field: message[:1900]}).encode("utf-8")
    request = urllib.request.Request(
        WEBHOOK_URL,
        data=body,
        # Discord rejects Python's default User-Agent, so send our own.
        headers={"Content-Type": "application/json", "User-Agent": "medallion-pipeline-alerts/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15):
            pass
        return True
    except Exception as exc:
        print(f"ALERT delivery failed: {exc}")
        return False


if __name__ == "__main__":
    ok = send(" ".join(sys.argv[1:]) or "Test alert from the medallion pipeline")
    print("sent" if ok else "not sent")