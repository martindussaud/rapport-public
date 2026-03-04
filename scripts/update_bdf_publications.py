#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Met à jour rapports.json avec les dernières publications de la Banque de France.

Le script tente d'extraire les publications depuis la page publique :
https://www.banque-france.fr/fr/publications-et-statistiques/publications

Stratégie :
- téléchargement HTML (urllib, sans dépendance externe)
- extraction prioritaire du JSON-LD (souvent plus stable que le markup visuel)
- fallback sur des liens HTML pertinents
- normalisation du format attendu par rapports.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

BDF_PUBLICATIONS_URL = "https://www.banque-france.fr/fr/publications-et-statistiques/publications"
DEFAULT_RAPPORTS_JSON = "rapports.json"
INSTITUTION = "Banque de France"


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

    iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if iso:
        yyyy, mm, dd = iso.groups()
        return f"{yyyy}-{mm}-{dd}"

    fr_slash = re.match(r"^(\d{2})/(\d{2})/(\d{4})", s)
    if fr_slash:
        dd, mm, yyyy = fr_slash.groups()
        return f"{yyyy}-{mm}-{dd}"

    fr_textual = re.match(
        r"^(\d{1,2})\s+([A-Za-zéèêëàâîïôöùûüç]+)\s+(\d{4})$", s.lower()
    )
    if fr_textual:
        dd, month_name, yyyy = fr_textual.groups()
        mm = FRENCH_MONTHS.get(month_name)
        if mm:
            return f"{yyyy}-{mm}-{int(dd):02d}"

    return None


def _download_text(url: str, timeout: int = 60) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "rapport-public-bot/1.0 (+https://github.com)",
            "Accept": "text/html,application/xhtml+xml,application/json,*/*",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
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


def _cleanup_html_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw)
    return _norm(unescape(text))


def _publication_item(title: str, date: Optional[str], url: str, description: str = "") -> Optional[Dict[str, str]]:
    title = _norm(title)
    url = _norm(url)
    parsed_date = _parse_date(_norm(date or ""))

    if not title or not url or not parsed_date:
        return None

    return {
        "title": title,
        "institution": INSTITUTION,
        "date": parsed_date,
        "url": url,
        "description": _norm(description) or f"Publication {INSTITUTION} du {parsed_date}",
    }


def _iter_jsonld_payloads(html: str) -> Iterable[Any]:
    pattern = re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        raw_json = match.group(1).strip()
        if not raw_json:
            continue
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        yield payload


def _extract_from_jsonld_object(obj: Dict[str, Any], page_url: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []

    candidates: List[Dict[str, Any]] = []
    if isinstance(obj.get("itemListElement"), list):
        for elt in obj["itemListElement"]:
            if isinstance(elt, dict):
                item = elt.get("item") if isinstance(elt.get("item"), dict) else elt
                if isinstance(item, dict):
                    candidates.append(item)
    else:
        candidates.append(obj)

    for candidate in candidates:
        title = _norm(str(candidate.get("headline") or candidate.get("name") or ""))
        url = _norm(str(candidate.get("url") or candidate.get("@id") or ""))
        date = _norm(str(candidate.get("datePublished") or candidate.get("dateCreated") or ""))
        description = _norm(str(candidate.get("description") or ""))

        if url:
            url = urljoin(page_url, url)

        item = _publication_item(title=title, date=date, url=url, description=description)
        if item:
            items.append(item)

    return items


def _extract_from_jsonld(html: str, page_url: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []

    for payload in _iter_jsonld_payloads(html):
        if isinstance(payload, dict):
            results.extend(_extract_from_jsonld_object(payload, page_url))
        elif isinstance(payload, list):
            for obj in payload:
                if isinstance(obj, dict):
                    results.extend(_extract_from_jsonld_object(obj, page_url))

    return results


def _extract_from_html_links(html: str, page_url: str) -> List[Dict[str, str]]:
    """Fallback minimaliste si JSON-LD absent."""
    # Cherche des blocs contenant une date française proche d'un lien vers publication.
    blocks = re.findall(r"<article\b.*?</article>|<li\b.*?</li>", html, flags=re.IGNORECASE | re.DOTALL)
    if not blocks:
        blocks = [html]

    out: List[Dict[str, str]] = []
    link_re = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', flags=re.IGNORECASE | re.DOTALL)
    date_re = re.compile(
        r"(\d{1,2}\s+[A-Za-zéèêëàâîïôöùûüç]+\s+\d{4}|\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})",
        flags=re.IGNORECASE,
    )

    for block in blocks:
        date_match = date_re.search(_cleanup_html_text(block))
        date_text = date_match.group(1) if date_match else ""
        for href, label_html in link_re.findall(block):
            url = urljoin(page_url, _norm(href))
            if "/publications" not in url:
                continue
            title = _cleanup_html_text(label_html)
            if len(title) < 12:
                continue
            item = _publication_item(title=title, date=date_text, url=url)
            if item:
                out.append(item)

    return out


def _today_in_paris_iso() -> str:
    return datetime.now(ZoneInfo("Europe/Paris")).date().isoformat()


def fetch_bdf_publications(url: str = BDF_PUBLICATIONS_URL) -> List[Dict[str, str]]:
    html = _download_text(url)

    candidates = _extract_from_jsonld(html, url)
    if not candidates:
        candidates = _extract_from_html_links(html, url)

    # Dédoublonnage local
    dedup: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for item in candidates:
        dedup[make_key(item)] = item

    # Tri descendant par date
    values = list(dedup.values())
    values.sort(key=lambda r: (_norm(r.get("date", "")), _norm(r.get("title", "")).lower()), reverse=True)
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description="Met à jour rapports.json avec les publications Banque de France")
    parser.add_argument("--url", default=BDF_PUBLICATIONS_URL, help="URL source des publications")
    parser.add_argument("--rapports", default=os.environ.get("RAPPORTS_JSON", DEFAULT_RAPPORTS_JSON), help="Chemin vers rapports.json")
    parser.add_argument("--limit", type=int, default=50, help="Nombre maximum de publications à ajouter")
    parser.add_argument("--today", default=_today_in_paris_iso(), help="Date ISO à conserver (par défaut: aujourd'hui Europe/Paris)")
    args = parser.parse_args()

    existing = load_existing(args.rapports)
    existing_keys = {make_key(r) for r in existing if isinstance(r, dict)}

    try:
        candidates = fetch_bdf_publications(args.url)
    except (URLError, HTTPError) as exc:
        print(f"Impossible de récupérer les publications BDF ({args.url}) : {exc}", file=sys.stderr)
        return 2

    if not candidates:
        print("Aucune publication BDF détectée.", file=sys.stderr)
        return 2

    today = _parse_date(args.today)
    if not today:
        print(f"Date invalide pour --today: {args.today}", file=sys.stderr)
        return 2

    today_candidates = [r for r in candidates if _norm(str(r.get("date", ""))) == today]
    if not today_candidates:
        print(f"Aucune publication BDF trouvée pour la date {today}.")
        return 0

    limited = today_candidates[: max(0, args.limit)]
    new_items = [r for r in limited if make_key(r) not in existing_keys]

    if not new_items:
        print(f"Aucune nouvelle publication BDF détectée pour la date {today}.")
    limited = candidates[: max(0, args.limit)]
    new_items = [r for r in limited if make_key(r) not in existing_keys]

    if not new_items:
        print("Aucune nouvelle publication BDF détectée.")
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

    with open(args.rapports, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Ajouté {len(new_items)} publication(s) BDF. Total: {len(merged)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
