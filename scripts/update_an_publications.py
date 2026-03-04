#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Met à jour rapports.json à partir des publications de l'Assemblée nationale.

Objectif principal: ne pas casser le workflow GitHub Actions si une source AN
change ou tombe temporairement.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

AN_SOURCES: List[Tuple[str, str]] = [
    ("https://www.assemblee-nationale.fr/dyn/opendata/list-publication/publication_j", "mixed"),
    # endpoint historique (peut renvoyer 404 selon les évolutions de l'API)
    ("https://data.assemblee-nationale.fr/api/document?type=rapport&num_page=1&num_items=200", "json"),
    # fallback plus tolérant sur certains environnements
    ("https://data.assemblee-nationale.fr/api/document?type=rapport", "json"),
]
DEFAULT_RAPPORTS_JSON = "rapports.json"


def _norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _parse_date(s: str) -> Optional[str]:
    s = _norm(s)
    if not s:
        return None

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    return None


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
    include_terms = ["rapport", "rapport d'information", "mission d'information", "commission d'enquête"]
    exclude_terms = ["footer", "pied de page", "plan du site", "mentions légales", "contact"]
    return any(t in b for t in include_terms) and not any(t in b for t in exclude_terms)


def _to_report_from_row(row: Dict[str, Any]) -> Optional[Dict[str, str]]:
    title = _best_field(
        row,
        [
            "titre",
            "title",
            "libelle",
            "intitule",
            "objet",
            "titre_publication",
            "nom",
        ],
    ) or ""
    date_raw = _best_field(
        row,
        [
            "date",
            "date_publication",
            "datepublication",
            "publication",
            "published",
            "dat",
            "date_creation",
        ],
    ) or ""
    url = _best_field(
        row,
        ["url", "lien", "link", "uri", "adresse", "permalink", "url_publication", "lien_publication"],
    ) or ""
    description = _best_field(row, ["resume", "résumé", "description", "descriptif", "contenu"]) or ""

    blob = " ".join(_norm(str(v)) for v in row.values())
    if not title:
        # fallback: si pas de champ titre explicite, on tente d'utiliser le blob (tronqué)
        title = _norm(blob)[:180]
    if not title or not url:
        return None
    if not _looks_like_report_blob(f"{title} {blob}"):
        return None

    date = _parse_date(date_raw)
    if not date:
        return None

    if url.startswith("/"):
        url = f"https://www.assemblee-nationale.fr{url}"

    return {
        "title": title,
        "institution": "Assemblée nationale",
        "date": date,
        "url": url,
        "description": description or f"Rapport parlementaire publié le {date}",
    }


def _to_report_from_api(doc: Dict[str, Any]) -> Optional[Dict[str, str]]:
    title = _norm(str(doc.get("titre") or doc.get("title") or ""))
    uri = _norm(str(doc.get("uri") or doc.get("url") or doc.get("lien") or ""))
    date = _parse_date(_norm(str(doc.get("date") or doc.get("date_publication") or "")))
    description = _norm(str(doc.get("resume") or doc.get("description") or ""))

    if not title or not uri or not date:
        return None
    if not _looks_like_report_blob(f"{title} {description}"):
        return None

    if uri.startswith(("http://", "https://")):
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


def _fetch_candidates() -> List[Dict[str, str]]:
    errors: List[str] = []

    for url, mode in AN_SOURCES:
        try:
            text = _download_text(url)
        except (URLError, HTTPError) as exc:
            errors.append(f"{url}: {exc}")
            continue

        candidates = _parse_json_reports(text) if mode == "json" else (_parse_csv_reports(text) or _parse_json_reports(text))
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
        # Important: on n'échoue pas le workflow si la source AN est indisponible.
        print("Aucun rapport AN détecté depuis les sources configurées. On conserve rapports.json inchangé.")
        return 0

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
