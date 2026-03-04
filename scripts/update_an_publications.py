#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Met à jour rapports.json à partir des publications de l'Assemblée nationale.

Cette version est volontairement robuste :
- pas de dépendance externe (urllib au lieu de requests)
- accepte plusieurs formats de réponse (CSV ou JSON)
- filtre les contenus non pertinents (navigation, footer, etc.)
- normalise les champs au format attendu par index.html
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
from datetime import datetime
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

AN_LISTING_URL = "https://www2.assemblee-nationale.fr/documents/liste?type=rapports-information&legis=17"
AN_PUBLICATION_J = "https://www.assemblee-nationale.fr/dyn/opendata/list-publication/publication_j"
AN_API_DOCUMENT = "https://data.assemblee-nationale.fr/api/document?type=rapport&num_page=1&num_items=200"
DEFAULT_RAPPORTS_JSON = "rapports.json"

FRENCH_MONTHS = {
    "janvier": "01",
    "février": "02",
    "fevrier": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "août": "08",
    "aout": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "décembre": "12",
    "decembre": "12",
}


def _norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _parse_date(s: str) -> Optional[str]:
    s = _norm(s)
    if not s:
        return None

    patterns = [
        r"^(\d{4})-(\d{2})-(\d{2})",  # YYYY-MM-DD
        r"^(\d{2})/(\d{2})/(\d{4})",  # DD/MM/YYYY
    ]

    m = re.match(patterns[0], s)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    m = re.match(patterns[1], s)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    m = re.match(r"^(\d{1,2})\s+([A-Za-zéèêëàâîïôöùûüç]+)\s+(\d{4})$", s.lower())
    if m:
        dd, month_name, yyyy = m.groups()
        mm = FRENCH_MONTHS.get(month_name)
        if mm:
            return f"{yyyy}-{mm}-{int(dd):02d}"

    return None


def _cleanup_html_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw)
    return _norm(unescape(text))


def _best_field(row: Dict[str, Any], candidates: Iterable[str]) -> Optional[str]:
    lower_to_real = {str(k).lower(): k for k in row.keys()}
    for candidate in candidates:
        key = lower_to_real.get(candidate.lower())
        if key is None:
            continue
        value = _norm(str(row.get(key, "")))
        if value:
            return value
    return None


def _looks_like_report_blob(blob: str) -> bool:
    b = blob.lower()
    include_terms = [
        "rapport",
        "rapport d'information",
        "mission d'information",
        "commission d'enquête",
    ]
    exclude_terms = [
        "footer",
        "pied de page",
        "plan du site",
        "mentions légales",
        "contact",
    ]
    return any(t in b for t in include_terms) and not any(t in b for t in exclude_terms)


def _to_report_from_row(row: Dict[str, Any]) -> Optional[Dict[str, str]]:
    title = _best_field(row, ["titre", "title", "libelle", "intitule", "objet"]) or ""
    date_raw = _best_field(row, ["date", "date_publication", "datepublication", "publication", "published", "dat"]) or ""
    url = _best_field(row, ["url", "lien", "link", "uri", "adresse", "permalink"]) or ""
    description = _best_field(row, ["resume", "résumé", "description", "descriptif"]) or ""

    if not title or not url:
        return None

    blob = " ".join(_norm(str(v)) for v in row.values())
    if not _looks_like_report_blob(blob):
        return None

    date = _parse_date(date_raw)
    if not date:
        return None

    return {
        "title": title,
        "institution": "Assemblée nationale",
        "date": date,
        "url": url,
        "description": description or f"Rapport parlementaire publié le {date}",
    }


def _to_report_from_api(doc: Dict[str, Any]) -> Optional[Dict[str, str]]:
    title = _norm(str(doc.get("titre", "")))
    uri = _norm(str(doc.get("uri", "")))
    date = _parse_date(_norm(str(doc.get("date", ""))))
    description = _norm(str(doc.get("resume", "")))

    if not title or not uri or not date:
        return None
    if not _looks_like_report_blob(f"{title} {description}"):
        return None

    if uri.startswith("http://") or uri.startswith("https://"):
        url = uri
    else:
        url = f"https://data.assemblee-nationale.fr{uri}"

    return {
        "title": title,
        "institution": "Assemblée nationale",
        "date": date,
        "url": url,
        "description": description or f"Rapport parlementaire publié le {date}",
    }


def load_existing(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def make_key(r: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        _norm(str(r.get("title", ""))).lower(),
        _norm(str(r.get("date", ""))),
        _norm(str(r.get("url", ""))),
    )


def _download_text(url: str, timeout: int = 60) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "rapport-public-bot/1.0 (+https://github.com)",
            "Accept": "application/json,text/csv,text/plain,*/*",
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        content = resp.read()

    for encoding in ("utf-8", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _parse_csv_reports(text: str) -> List[Dict[str, str]]:
    def parse(delimiter: str) -> List[Dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        return [dict(r) for r in reader if any(_norm(str(v)) for v in r.values())]

    rows = parse(";")
    if len(rows) <= 1:
        rows = parse(",")

    return [rep for rep in (_to_report_from_row(r) for r in rows) if rep]


def _parse_json_reports(text: str) -> List[Dict[str, str]]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []

    documents: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        maybe_docs = payload.get("documents")
        if isinstance(maybe_docs, list):
            documents = [d for d in maybe_docs if isinstance(d, dict)]
    elif isinstance(payload, list):
        documents = [d for d in payload if isinstance(d, dict)]

    return [rep for rep in (_to_report_from_api(d) for d in documents) if rep]


def _parse_html_listing_reports(text: str, source_url: str) -> List[Dict[str, str]]:
    blocks = re.findall(r"<article\b.*?</article>|<li\b.*?</li>|<tr\b.*?</tr>", text, flags=re.IGNORECASE | re.DOTALL)
    if not blocks:
        blocks = [text]

    link_re = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', flags=re.IGNORECASE | re.DOTALL)
    date_re = re.compile(
        r"(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\s+[A-Za-zéèêëàâîïôöùûüç]+\s+\d{4})",
        flags=re.IGNORECASE,
    )

    out: List[Dict[str, str]] = []
    for block in blocks:
        block_text = _cleanup_html_text(block)
        date_match = date_re.search(block_text)
        date = _parse_date(date_match.group(1)) if date_match else None
        if not date:
            continue

        for href, label_html in link_re.findall(block):
            if href.startswith("javascript:") or href.startswith("#"):
                continue
            label = _cleanup_html_text(label_html)
            if len(label) < 15:
                continue

            url = urljoin(source_url, _norm(href))
            if "assemblee-nationale.fr" not in url:
                continue

            out.append(
                {
                    "title": label,
                    "institution": "Assemblée nationale",
                    "date": date,
                    "url": url,
                    "description": f"Rapport d'information publié le {date}",
                }
            )
            break

    dedup: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for row in out:
        dedup[make_key(row)] = row
    return list(dedup.values())


def _fetch_candidates() -> List[Dict[str, str]]:
    errors: List[str] = []

    for url, mode in ((AN_LISTING_URL, "html"), (AN_PUBLICATION_J, "mixed"), (AN_API_DOCUMENT, "json")):
        try:
            text = _download_text(url)
        except (URLError, HTTPError) as exc:
            errors.append(f"{url}: {exc}")
            continue

        candidates: List[Dict[str, str]] = []
        if mode == "html":
            candidates = _parse_html_listing_reports(text, url)
        elif mode == "json":
            candidates = _parse_json_reports(text)
        else:
            candidates = _parse_csv_reports(text)
            if not candidates:
                candidates = _parse_json_reports(text)

        if candidates:
            return candidates

    if errors:
        print("Impossible de récupérer les données AN :", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)
    return []


def main() -> int:
    rapports_path = os.environ.get("RAPPORTS_JSON", DEFAULT_RAPPORTS_JSON)

    existing = load_existing(rapports_path)
    existing_keys = {make_key(r) for r in existing if isinstance(r, dict)}

    candidates = _fetch_candidates()
    if not candidates:
        print("Aucun rapport AN détecté depuis les sources configurées.", file=sys.stderr)
        return 2

    # Limite de sécurité pour éviter des ajouts massifs involontaires
    candidates = candidates[:200]

    new_items = [r for r in candidates if make_key(r) not in existing_keys]
    if not new_items:
        print("Aucun nouveau rapport AN détecté.")
        return 0

    merged = existing + new_items

    def sort_key(r: Dict[str, Any]) -> Tuple[datetime, str]:
        date_s = _norm(str(r.get("date", "")))
        try:
            dt = datetime.fromisoformat(date_s)
        except ValueError:
            dt = datetime.min
        return (dt, _norm(str(r.get("title", "")).lower()))

    merged.sort(key=sort_key, reverse=True)

    with open(rapports_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Ajouté {len(new_items)} rapport(s) AN. Total: {len(merged)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
