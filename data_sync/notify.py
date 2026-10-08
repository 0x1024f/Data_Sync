"""HTTP notification transport. Each call makes exactly one POST attempt."""
import json
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class NotificationError(Exception):
    """Only a sanitized error code is carried into logs and the ledger."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def post_notification(url, payload, timeout):
    request = Request(url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                      headers={"Content-Type": "application/json"}, method="POST")
    try:
        # Do not inherit proxy settings or turn redirects into additional requests.
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=timeout) as response:
            if not 200 <= response.status < 300:
                raise NotificationError("HTTP_" + str(response.status))
            body = response.read(1024 * 1024 + 1)
            if len(body) > 1024 * 1024:
                raise NotificationError("ResponseTooLarge")
    except HTTPError as error:
        error.close()
        raise NotificationError("HTTP_" + str(error.code)) from None
    except (URLError, OSError):
        raise NotificationError("ConnectionOrTimeout") from None
    try:
        result = json.loads(body)
    except (ValueError, UnicodeError):
        raise NotificationError("InvalidJSON") from None
    if not isinstance(result, dict) or type(result.get("code")) not in (int, float) or result["code"] != 200:
        raise NotificationError("BusinessFailure")
