import os
import re
from datetime import datetime
from urllib.parse import unquote, urljoin, urlparse

import requests


PLACES_TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

max_details_requests = 5


_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
_COPYRIGHT_RE = re.compile(
    r"(?:copyright|\(c\)|©)\s*(?:\d{4}\s*[-–—]\s*)?(19\d{2}|20\d{2})",
    re.IGNORECASE,
)


def _fetch_html(url: str, *, timeout: float = 8.0) -> str:
    try:
        print(f"[http] GET {url} timeout={timeout}")
        resp = requests.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": "lead-scraper/1.0"},
        )
        print(f"[http] status={resp.status_code} final_url={resp.url}")
        if resp.status_code != 200:
            return ""
        return resp.text or ""
    except Exception:
        _print_traceback()
        return ""


def analyze_website(url: str) -> dict:
    result = {
        "reachable": False,
        "final_url": "",
        "has_ssl": False,
        "estimated_year": None,
        "platform": "unknown",
        "is_outdated": True,
        "notes": "",
    }

    base = (url or "").strip()
    if not base:
        result["notes"] = "no url"
        return result

    try:
        print(f"[site] analyze_website GET {base}")
        resp = requests.get(
            base,
            timeout=8.0,
            allow_redirects=True,
            headers={"User-Agent": "lead-scraper/1.0"},
        )
        print(f"[site] status={resp.status_code} final_url={resp.url}")
        final_url = (resp.url or base).strip()
        html = resp.text or ""

        result["final_url"] = final_url
        result["has_ssl"] = final_url.lower().startswith("https://")
        result["reachable"] = resp.status_code == 200 and bool(html.strip())

        if not result["reachable"]:
            result["notes"] = f"http {resp.status_code}" if resp is not None else "unreachable"
            result["is_outdated"] = True
            return result

        lower = html.lower()

        platform = "unknown"
        if "wp-content" in lower or "wordpress" in lower:
            platform = "wordpress"
        elif "wix.com" in lower or "wixsite" in lower or "x-wix" in lower:
            platform = "wix"
        elif "squarespace" in lower:
            platform = "squarespace"
        elif "cdn.shopify.com" in lower or "shopify" in lower:
            platform = "shopify"
        elif "joomla" in lower:
            platform = "joomla"
        result["platform"] = platform

        has_viewport = ("<meta" in lower and "name=\"viewport\"" in lower) or ("name='viewport'" in lower)

        current_year = datetime.utcnow().year

        years: list[int] = []

        for y in _COPYRIGHT_RE.findall(html):
            try:
                years.append(int(y))
            except Exception:
                continue

        if not years:
            footer_slice = lower[-5000:] if len(lower) > 5000 else lower
            for y in _YEAR_RE.findall(footer_slice):
                try:
                    years.append(int(y))
                except Exception:
                    continue

        years = [y for y in years if 1995 <= y <= current_year + 1]
        estimated_year = max(years) if years else None
        result["estimated_year"] = estimated_year

        outdated_reasons: list[str] = []
        if not result["has_ssl"]:
            outdated_reasons.append("no ssl")
        if not has_viewport:
            outdated_reasons.append("no viewport")
        if estimated_year is not None and current_year - estimated_year >= 4:
            outdated_reasons.append("old year")
        if platform in {"joomla"}:
            outdated_reasons.append("old platform")

        old_markers = [
            "x-ua-compatible",
            "jquery-1.",
            "jquery/1.",
            "wp-includes/js/jquery/jquery.js?ver=1.",
            "document.all",
        ]
        if any(m in lower for m in old_markers):
            outdated_reasons.append("old tech")

        result["is_outdated"] = bool(outdated_reasons)
        result["notes"] = ", ".join(outdated_reasons) if outdated_reasons else "ok"
        return result
    except Exception:
        result["reachable"] = False
        result["final_url"] = base
        result["has_ssl"] = base.lower().startswith("https://")
        result["estimated_year"] = None
        result["platform"] = "unknown"
        result["is_outdated"] = True
        result["notes"] = "unreachable"
        return result


def _is_bad_email(email: str) -> bool:
    e = (email or "").strip().lower()
    if not e:
        return True
    if ".." in e:
        return True
    if "@" not in e:
        return True

    local, domain = e.rsplit("@", 1)
    local = local.strip()
    domain = domain.strip().lstrip("www.")
    if not local or not domain:
        return True

    if local in {"test", "example", "name", "demo", "user"}:
        return True
    if domain in {"domain.com", "example.com", "example.org", "example.net", "test.com"}:
        return True

    if any(domain.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico")):
        return True
    if e.startswith("noreply@") or e.startswith("no-reply@") or e.startswith("donotreply@"): 
        return True
    if e.startswith("example@"): 
        return True
    if e.endswith("@example.com") or e.endswith("@example.org") or e.endswith("@example.net"):
        return True
    if e.endswith("@email.com"):
        return True

    bad_domains = (
        "facebook.com",
        "facebookmail.com",
        "instagram.com",
        "twitter.com",
        "tiktok.com",
        "linkedin.com",
        "youtube.com",
        "pinterest.com",
    )
    if any(e.endswith("@" + d) or ("@" + d) in e for d in bad_domains):
        return True
    return False


def _extract_first_email(html: str) -> str:
    if not html:
        return ""
    for m in _EMAIL_RE.findall(html):
        email = (m or "").strip()
        if not _is_bad_email(email):
            return email
    return ""


def _extract_emails_from_html(html: str) -> list[str]:
    if not html:
        return []

    candidates: list[str] = []

    for m in _EMAIL_RE.findall(html):
        e = (m or "").strip()
        if e:
            candidates.append(e)

    for m in re.findall(r"mailto:([^\"'<>\s]+)", html, flags=re.IGNORECASE):
        raw = (m or "").strip()
        if not raw or raw.startswith("?"):
            continue
        if "?" in raw:
            raw = raw.split("?", 1)[0]
        raw = unquote(raw)
        raw = raw.replace("%40", "@").replace("%2e", ".").replace("%2E", ".")
        raw = raw.strip().strip("<>")
        if raw:
            candidates.append(raw)

    seen: set[str] = set()
    out: list[str] = []
    for e in candidates:
        el = (e or "").strip().lower()
        if not el or el in seen:
            continue
        seen.add(el)
        out.append(e.strip())
    return out


def _pick_best_email(emails: list[str], base_host: str) -> str:
    if not emails:
        return ""

    cleaned: list[str] = []
    for e in emails:
        if _is_bad_email(e):
            continue
        el = e.lower()
        if any(x in el for x in ("facebook.com", "instagram.com", "twitter.com", "tiktok.com", "linkedin.com", "youtube.com", "pinterest.com")):
            continue
        if any(x in el for x in ("/share", "sharer.php", "intent/tweet", "addthis", "mailto:?")):
            continue
        cleaned.append(e)
    if not cleaned:
        return ""

    base = (base_host or "").lower().lstrip("www.")
    if base:
        for e in cleaned:
            try:
                dom = e.rsplit("@", 1)[-1].strip().lower().lstrip("www.")
            except Exception:
                continue
            if dom == base or dom.endswith("." + base):
                return e

    return cleaned[0]


def _find_email_on_website(website: str) -> str:
    base = (website or "").strip()
    if not base:
        return ""

    try:
        base_host = (urlparse(base).hostname or "").lower().lstrip("www.")
    except Exception:
        base_host = ""

    paths = [
        "",
        "contact",
        "contacts",
        "about",
        "impressum",
        "yhteystiedot",
        "kontakt",
        "privacy",
    ]

    collected: list[str] = []
    for path in paths:
        try:
            url = base
            if path:
                url = urljoin(base if base.endswith("/") else base + "/", path)
            html = _fetch_html(url, timeout=8.0)
            emails = _extract_emails_from_html(html)
            if emails:
                collected.extend(emails)
                best = _pick_best_email(collected, base_host)
                if best:
                    return best
        except Exception:
            continue

    return _pick_best_email(collected, base_host)


def _print_traceback() -> None:
    try:
        __import__("traceback").print_exc()
    except Exception:
        pass


def _safe_get(url: str, params: dict, *, timeout: float = 30.0) -> dict | None:
    try:
        print(f"[google] GET {url} timeout={timeout}")
        resp = requests.get(url, params=params, timeout=timeout)
        print(f"[google] status={resp.status_code} final_url={resp.url}")
        try:
            data = resp.json()
        except Exception:
            _print_traceback()
            data = None

        if resp.status_code != 200:
            print(f"HTTP {resp.status_code} {resp.url}")
            txt = resp.text or ""
            print(txt[:800])
            return None

        if isinstance(data, dict):
            status = data.get("status")
            if url == PLACES_TEXTSEARCH_URL:
                count = len(data.get("results", []) or [])
                print(f"[google] TextSearch status={status} results={count}")
            else:
                print(f"[google] Details status={status}")
            if status in {"REQUEST_DENIED", "OVER_DAILY_LIMIT"}:
                print("Google Places API error response:")
                print(resp.text)
        return data if isinstance(data, dict) else None
    except Exception:
        _print_traceback()
        return None


def _place_details(place_id: str, api_key: str) -> dict | None:
    params = {
        "place_id": place_id,
        "fields": "name,formatted_phone_number,website,formatted_address",
        "key": api_key,
    }
    return _safe_get(PLACES_DETAILS_URL, params)


def search_businesses(city: str, category: str, api_key: str | None = None, verify_nosite: bool = False) -> list[dict]:
    try:
        print(f"[search] city={city!r} category={category!r} verify_nosite={verify_nosite}")
        api_key = (api_key or os.getenv("GOOGLE_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY")

        if not isinstance(city, str) or not city.strip():
            return []
        if not isinstance(category, str) or not category.strip():
            return []

        query = f"{category.strip()} in {city.strip()}"
        print(f"[search] query={query!r}")
        payload = _safe_get(PLACES_TEXTSEARCH_URL, {"query": query, "key": api_key})
        if not payload:
            print("[search] Text Search returned no payload")
            return []

        status = payload.get("status")
        if status not in {"OK", "ZERO_RESULTS"}:
            print("Google Places Text Search raw response:")
            print(payload)
            return []

        text_items: list[dict] = list(payload.get("results", []) or [])
        print(f"[search] text_items={len(text_items)}")

        results: list[dict] = []
        for item in text_items:
            try:
                place_id = (item.get("place_id") or "").strip()
                if not place_id:
                    continue

                name = (item.get("name") or "").strip()
                if not name:
                    continue

                address = (item.get("formatted_address") or item.get("vicinity") or "").strip()

                results.append(
                    {
                        "place_id": place_id,
                        "name": name,
                        "phone": "",
                        "website": "",
                        "email": "",
                        "address": address,
                        "has_website": None,
                        "website_checked": False,
                        "website_status": "unknown",
                        "website_year_estimate": None,
                        "website_platform": "unknown",
                        "website_outdated": False,
                        "website_notes": "",
                        "_score": float(item.get("rating") or 0.0) * float(item.get("user_ratings_total") or 0.0),
                    }
                )
            except Exception:
                _print_traceback()
                continue

        if results:
            candidates = sorted(
                range(len(results)),
                key=lambda i: float(results[i].get("_score") or 0.0),
                reverse=True,
            )
            details_budget = 20 if verify_nosite else max_details_requests
            if details_budget < 0:
                details_budget = 0

            checked_count = 0
            confirmed_no_website = 0

            for idx in candidates[:details_budget]:
                try:
                    place_id = (results[idx].get("place_id") or "").strip()
                    if not place_id:
                        continue

                    details_timeout = 10.0 if verify_nosite else 30.0
                    params = {
                        "place_id": place_id,
                        "fields": "name,formatted_phone_number,website,formatted_address",
                        "key": api_key,
                    }
                    details = _safe_get(PLACES_DETAILS_URL, params, timeout=details_timeout)
                    checked_count += 1
                    if not details or details.get("status") != "OK":
                        if isinstance(details, dict) and details.get("status") in {"REQUEST_DENIED", "OVER_DAILY_LIMIT"}:
                            print("Google Places Details raw response:")
                            print(details)
                        continue

                    r = details.get("result") or {}
                    phone = (r.get("formatted_phone_number") or "").strip()
                    website = (r.get("website") or "").strip()
                    address = (r.get("formatted_address") or "").strip() or (results[idx].get("address") or "")
                    name = (r.get("name") or "").strip() or (results[idx].get("name") or "")

                    results[idx]["name"] = name
                    results[idx]["phone"] = phone
                    results[idx]["website"] = website
                    results[idx]["address"] = address

                    results[idx]["website_checked"] = True

                    if website:
                        if not verify_nosite:
                            website_analysis = analyze_website(website)
                            final_url = (
                                (website_analysis.get("final_url") or website).strip()
                                if isinstance(website_analysis, dict)
                                else website
                            )
                            results[idx]["email"] = _find_email_on_website(final_url)

                            if isinstance(website_analysis, dict) and website_analysis.get("reachable"):
                                results[idx]["website_status"] = "reachable"
                            else:
                                results[idx]["website_status"] = "broken"

                            if isinstance(website_analysis, dict):
                                results[idx]["website_year_estimate"] = website_analysis.get("estimated_year")
                                results[idx]["website_platform"] = (website_analysis.get("platform") or "unknown")
                                results[idx]["website_outdated"] = bool(website_analysis.get("is_outdated"))
                                results[idx]["website_notes"] = (website_analysis.get("notes") or "")

                        results[idx]["has_website"] = True
                    else:
                        results[idx]["has_website"] = False
                        results[idx]["website_status"] = "no_website"
                        results[idx]["website_year_estimate"] = None
                        results[idx]["website_platform"] = "unknown"
                        results[idx]["website_outdated"] = False
                        results[idx]["website_notes"] = ""
                        confirmed_no_website += 1
                except Exception:
                    _print_traceback()
                    continue

            if verify_nosite:
                print(f"[nosite] Checked candidates: {checked_count}")
                print(f"[nosite] Confirmed real no-website leads: {confirmed_no_website}")
                results = [
                    r
                    for r in results
                    if bool(r.get("website_checked")) and (r.get("has_website") is False)
                ]

        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for r in results:
            key = ((r.get("name") or "").strip().casefold(), (r.get("address") or "").strip().casefold())
            if key in seen:
                continue
            seen.add(key)
            r.pop("_score", None)
            r.pop("place_id", None)
            deduped.append(r)

        deduped.sort(key=lambda x: bool(x.get("website")))
        print(f"[search] returning={len(deduped)}")
        return deduped
    except Exception:
        _print_traceback()
        return []


if __name__ == "__main__":
    api_key = (os.getenv("GOOGLE_API_KEY") or "").strip()
    results = search_businesses("Helsinki", "restaurant", api_key)
    print("RESULT COUNT:", len(results))
    for r in results[:3]:
        print(r)
