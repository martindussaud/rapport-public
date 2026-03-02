#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Met à jour rapports.json à partir de la liste quotidienne des publications AN (CSV).
- Télécharge publication_j (au fil de l'eau)
- Filtre les lignes "rapport" (heuristique robuste)
- Normalise au format {title, institution, date, url, description}
- Merge + déduplication
- Ecrit rapports.json (tri par date desc)
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests


AN_PUBLICATION_J = "https://www.assemblee-nationale.fr/dyn/opendata/list-publication/publication_j"
DEFAULT_RAPPORTS_JSON = "rapports.json"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _parse_date(s: str) -> Optional[str]:
    s = _norm(s)
    if not s:
        return None

    # cas fréquents : YYYY-MM-DD
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)

    # cas fréquents : DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    # fallback: impossible à parser proprement
    return None


def _looks_like_report(row: Dict[str, str]) -> bool:
    """
    Heuristique : on cherche le mot "rapport" dans les colonnes texte.
    Le CSV AN peut évoluer; on scanne toutes les valeurs.
    """
    blob = " ".join(_norm(v).lower() for v in row.values() if v)
    # "rapport", "rapport d'information", "rapports", etc.
    return "rapport" in blob


def _best_field(row: Dict[str, str], candidates: List[str]) -> Optional[str]:
    keys_lower = {k.lower(): k for k in row.keys()}
    for c in candidates:
        k = keys_lower.get(c.lower())
        if k and _norm(row.get(k, "")):
            return _norm(row[k])
    return None


def _row_to_report(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    # Titre
    title = _best_field(row, ["titre", "title", "libelle", "intitule", "objet"])
    if not title:
        return None

    # Date
    date_raw = _best_field(row, ["date", "date_publication", "datepublication", "publication", "published", "dat"])
    date = _parse_date(date_raw or "")
    if not date:
        # si pas de date exploitable, on abandonne (sinon ton UI par année devient incohérente)
        return None

    # URL
    url = _best_field(row, ["url", "lien", "link", "uri", "adresse", "permalink"])
    if not url:
        return None

    desc = _best_field(row, ["resume", "résumé", "description", "descriptif"]) or f"Document publié le {date}"

    return {
        "title": title,
        "institution": "Assemblée nationale",
        "date": date,
        "url": url,
        "description": desc,
    }


def load_existing(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def make_key(r: Dict[str, Any]) -> Tuple[str, str, str]:
    return (_norm(r.get("title", "")).lower(), _norm(r.get("date", "")), _norm(r.get("url", "")))


def main() -> int:
    rapports_path = os.environ.get("RAPPORTS_JSON", DEFAULT_RAPPORTS_JSON)

    # 1) charger existant
    existing = load_existing(rapports_path)
    existing_keys = {make_key(r) for r in existing if isinstance(r, dict)}

    # 2) télécharger CSV publication_j
    resp = requests.get(AN_PUBLICATION_J, timeout=60)
    resp.raise_for_status()

    # Certaines sources renvoient du latin1; on tente utf-8 puis fallback
    content = resp.content
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    # 3) parse CSV (delimiter parfois ';' sur les exports FR)
    # On essaie d'abord ';' puis ',' si besoin.
    def parse_with(delim: str) -> List[Dict[str, str]]:
        reader = csv.DictReader(io.StringIO(text), delimiter=delim)
        return [dict(r) for r in reader if any((v or "").strip() for v in r.values())]

    rows = parse_with(";")
    if len(rows) <= 1:
        rows = parse_with(",")

    if not rows:
        print("Aucune ligne CSV lisible depuis publication_j", file=sys.stderr)
        return 2

    # 4) filtrer + normaliser
    candidates: List[Dict[str, Any]] = []
    for row in rows:
        if not _looks_like_report(row):
            continue
        rep = _row_to_report(row)
        if rep:
            candidates.append(rep)

    # On limite raisonnablement (publication_j peut être volumineux)
    candidates = candidates[:200]

    # 5) dédupe + merge
    new_items = [r for r in candidates if make_key(r) not in existing_keys]

    if not new_items:
        print("Aucun nouveau rapport AN détecté.")
        return 0

    merged = existing + new_items

    # 6) tri par date desc (puis titre)
    def sort_key(r: Dict[str, Any]):
        d = r.get("date", "")
        try:
            dt = datetime.fromisoformat(d)
        except Exception:
            dt = datetime.min
        return (dt, _norm(r.get("title", "")).lower())

    merged.sort(key=sort_key, reverse=True)

    with open(rapports_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"Ajouté {len(new_items)} rapport(s) AN. Total: {len(merged)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
