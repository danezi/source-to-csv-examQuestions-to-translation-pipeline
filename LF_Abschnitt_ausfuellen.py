#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pdfplumber
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field


# ----------------------------
# Ausgabestrukturen (Pydantic)
# ----------------------------

class ZeilenKlassifikation(BaseModel):
    row_index: int = Field(description="0-basierter Index der Zeile im ursprünglichen CSV-Batch")
    LFNr: str = Field(default="")
    LF: str = Field(default="")
    AbschnNr: str = Field(default="")
    Abschnitt: str = Field(default="")
    Confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class BatchKlassifikation(BaseModel):
    results: List[ZeilenKlassifikation]


# ----------------------------
# Konstanten
# ----------------------------

TEXTSPALTEN_STANDARD = [
    "Frage", "frage", "Question", "question",
    "A", "B", "C", "D",
    "Antwort", "Answer", "answer", "antwort",
]

PRUEF_SCHWELLENWERT = 0.75  # Confidence < Schwellenwert => NeedsReview = TRUE


# ----------------------------
# Protokollierung
# ----------------------------

def protokoll_einrichten(log_pfad: Path) -> logging.Logger:
    logger = logging.getLogger("klassifizierer")
    logger.setLevel(logging.DEBUG)

    format = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Protokolldatei
    datei_handler = logging.FileHandler(log_pfad, encoding="utf-8")
    datei_handler.setLevel(logging.DEBUG)
    datei_handler.setFormatter(format)

    # Konsolenausgabe
    konsole_handler = logging.StreamHandler(sys.stdout)
    konsole_handler.setLevel(logging.INFO)
    konsole_handler.setFormatter(format)

    logger.addHandler(datei_handler)
    logger.addHandler(konsole_handler)
    return logger


# ----------------------------
# Hilfsfunktionen
# ----------------------------

def argumente_parsen() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Klassifiziert CSV-Fragen in Lernfelder anhand eines PDFs und der OpenAI API."
    )
    parser.add_argument("--pdf", required=True, help="Pfad zur Referenz-PDF")
    parser.add_argument("--csv", required=True, help="Pfad zur Eingabe-CSV")
    parser.add_argument("--prompt", required=True, help="Pfad zur Prompt-Textdatei")
    parser.add_argument("--out", default=None, help="Pfad zur Ausgabe-CSV (Standard: <csv-name>_classified.csv)")
    parser.add_argument("--model", default="gpt-4o", help="OpenAI-Modellname")
    parser.add_argument("--batch-groesse", type=int, default=20, help="Zeilen pro API-Aufruf")
    parser.add_argument("--max-versuche", type=int, default=3, help="Maximale Wiederholungen pro Batch")
    parser.add_argument("--parallelitaet", type=int, default=5, help="Anzahl gleichzeitiger API-Aufrufe (Standard: 5)")
    parser.add_argument(
        "--pruef-schwelle", type=float, default=PRUEF_SCHWELLENWERT,
        help="Confidence-Schwellenwert für NeedsReview=TRUE (Standard: 0.75)"
    )
    return parser.parse_args()


def umgebung_laden() -> None:
    """Lädt Umgebungsvariablen aus der .env-Datei (falls vorhanden)."""
    env_pfad = Path(".env")
    if env_pfad.exists():
        load_dotenv(env_pfad)
    if not os.getenv("OPENAI_API_KEY"):
        print("Fehler: OPENAI_API_KEY ist nicht gesetzt (weder in .env noch als Umgebungsvariable).", file=sys.stderr)
        sys.exit(1)


def prompt_lesen(prompt_pfad: Path) -> str:
    return prompt_pfad.read_text(encoding="utf-8").strip()


def pdf_text_extrahieren(pdf_pfad: Path) -> str:
    """Extrahiert den gesamten Text aus dem PDF, Seite für Seite."""
    seiten = []
    with pdfplumber.open(pdf_pfad) as pdf:
        for i, seite in enumerate(pdf.pages, start=1):
            text = seite.extract_text() or ""
            if text.strip():
                seiten.append(f"--- Seite {i} ---\n{text}")
    return "\n\n".join(seiten)


def csv_flexibel_lesen(csv_pfad: Path) -> pd.DataFrame:
    """Versucht verschiedene Encodings und erkennt das Trennzeichen automatisch."""
    encodings = ["utf-8-sig", "utf-8", "cp1252"]
    trennzeichen = [";", ",", "\t"]
    letzter_fehler = None
    for enc in encodings:
        for sep in trennzeichen:
            try:
                df = pd.read_csv(csv_pfad, encoding=enc, sep=sep, dtype=str, keep_default_na=False)
                if len(df.columns) > 1:
                    return df
            except Exception as e:
                letzter_fehler = e
    raise RuntimeError(f"CSV konnte nicht gelesen werden: {letzter_fehler}")


def textspalten_erkennen(df: pd.DataFrame) -> List[str]:
    spalten = [s for s in TEXTSPALTEN_STANDARD if s in df.columns]
    if spalten:
        return spalten
    # Fallback: erste 2 Spalten
    return list(df.columns[:2])


def batch_nutzdaten_erstellen(df_batch: pd.DataFrame, textspalten: List[str]) -> List[dict]:
    zeilen = []
    for idx, zeile in df_batch.iterrows():
        zeilen.append({
            "row_index": int(idx),
            "klassifikationstext": " | ".join(
                f"{sp}: {str(zeile.get(sp, ''))}"
                for sp in textspalten
                if str(zeile.get(sp, "")).strip()
            ),
        })
    return zeilen


def batch_klassifizieren(
    client: OpenAI,
    model: str,
    pdf_text: str,
    nutzer_prompt: str,
    zeilen_nutzdaten: List[dict],
    max_versuche: int,
    logger: logging.Logger,
) -> BatchKlassifikation:
    batch_json = json.dumps(zeilen_nutzdaten, ensure_ascii=False)

    system_anweisung = (
        "Du bist ein Experte für deutsche Berufsausbildungs-Rahmenlehrpläne. "
        "Der folgende PDF-Text ist dein Referenzdokument. "
        "Für jede Zeile wählst du das passendste Lernfeld und den passendsten Abschnitt. "
        "Verändere keine Originaldaten. "
        "Gib ausschließlich die geforderte JSON-Struktur zurück. "
        "Falls ein exakter Abschnitt nicht erkennbar ist, wähle den thematisch nächsten. "
        "Confidence muss eine Dezimalzahl zwischen 0 und 1 sein.\n\n"
        f"=== PDF-INHALT ===\n{pdf_text}"
    )

    nutzer_nachricht = (
        f"{nutzer_prompt}\n\n"
        "Hier sind die zu klassifizierenden Zeilen im JSON-Format.\n"
        "row_index entspricht dem ursprünglichen Zeilenindex in der CSV-Datei.\n\n"
        f"{batch_json}"
    )

    letzter_fehler: Optional[Exception] = None

    for versuch in range(1, max_versuche + 1):
        try:
            antwort = client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": system_anweisung},
                    {"role": "user", "content": nutzer_nachricht},
                ],
                response_format=BatchKlassifikation,
            )
            ergebnis = antwort.choices[0].message.parsed
            if ergebnis is None:
                raise RuntimeError("Das Modell hat kein gültiges JSON zurückgegeben (parsed=None)")
            return ergebnis
        except Exception as e:
            letzter_fehler = e
            logger.warning(f"Versuch {versuch}/{max_versuche} fehlgeschlagen: {e}")
            if versuch < max_versuche:
                time.sleep(2 * versuch)

    raise RuntimeError(f"Klassifizierung nach {max_versuche} Versuchen fehlgeschlagen: {letzter_fehler}")


def ergebnisse_anwenden(df: pd.DataFrame, batch_ergebnis: BatchKlassifikation, schwelle: float) -> None:
    for eintrag in batch_ergebnis.results:
        idx = eintrag.row_index
        if idx not in df.index:
            continue
        df.at[idx, "LFNr"] = eintrag.LFNr
        df.at[idx, "LF"] = eintrag.LF
        df.at[idx, "AbschnNr"] = eintrag.AbschnNr
        df.at[idx, "Abschnitt"] = eintrag.Abschnitt
        df.at[idx, "Confidence"] = str(eintrag.Confidence)
        df.at[idx, "NeedsReview"] = "TRUE" if eintrag.Confidence < schwelle else "FALSE"


def zwischenspeichern(df: pd.DataFrame, ausgabe_pfad: Path) -> None:
    """Speichert den aktuellen Stand atomar: erst in .tmp schreiben, dann umbenennen.
    So bleibt die Ausgabedatei immer konsistent, auch bei einem Programmabbruch."""
    temp_pfad = ausgabe_pfad.with_suffix(".tmp")
    df.to_csv(temp_pfad, index=False, encoding="utf-8-sig")
    temp_pfad.replace(ausgabe_pfad)  # atomare Umbenennung


def bereits_klassifizierte_batches_ermitteln(df: pd.DataFrame, batch_groesse: int) -> set:
    """Gibt die Indizes der Batches zurück, bei denen alle Zeilen bereits LFNr haben."""
    fertig = set()
    gesamt = len(df)
    for batch_nr, start in enumerate(range(0, gesamt, batch_groesse)):
        ende = min(start + batch_groesse, gesamt)
        batch_df = df.iloc[start:ende]
        if batch_df["LFNr"].replace("", pd.NA).notna().all():
            fertig.add(batch_nr)
    return fertig


# ----------------------------
# Hauptprogramm
# ----------------------------

def main() -> None:
    args = argumente_parsen()
    umgebung_laden()

    pdf_pfad = Path(args.pdf)
    csv_pfad = Path(args.csv)
    prompt_pfad = Path(args.prompt)
    ausgabe_pfad = Path(args.out) if args.out else csv_pfad.with_name(csv_pfad.stem + "_classified" + csv_pfad.suffix)

    # Protokolldatei neben der Ausgabe-CSV
    log_pfad = ausgabe_pfad.with_suffix(".log")
    logger = protokoll_einrichten(log_pfad)

    logger.info(f"=== Programmstart: {datetime.now().isoformat()} ===")
    logger.info(f"PDF        : {pdf_pfad}")
    logger.info(f"CSV        : {csv_pfad}")
    logger.info(f"Ausgabe    : {ausgabe_pfad}")
    logger.info(f"Protokoll  : {log_pfad}")

    for pfad, bezeichnung in [(pdf_pfad, "PDF"), (csv_pfad, "CSV"), (prompt_pfad, "Prompt")]:
        if not pfad.exists():
            logger.error(f"{bezeichnung} nicht gefunden: {pfad}")
            sys.exit(1)

    client = OpenAI()

    prompt_text = prompt_lesen(prompt_pfad)
    logger.info("PDF-Text wird extrahiert...")
    pdf_text = pdf_text_extrahieren(pdf_pfad)
    logger.info(f"PDF extrahiert: {len(pdf_text)} Zeichen")

    # Ausgabe-CSV laden falls vorhanden (Wiederaufnahme), sonst Quell-CSV
    if ausgabe_pfad.exists():
        logger.info(f"Wiederaufnahme erkannt: Lade '{ausgabe_pfad}'")
        df = csv_flexibel_lesen(ausgabe_pfad)
    else:
        df = csv_flexibel_lesen(csv_pfad)

    # Zielspalten hinzufügen falls nicht vorhanden
    for spalte in ["LFNr", "LF", "AbschnNr", "Abschnitt", "Confidence", "NeedsReview"]:
        if spalte not in df.columns:
            df[spalte] = ""

    textspalten = textspalten_erkennen(df)
    logger.info(f"Verwendete Textspalten: {textspalten}")

    gesamt_zeilen = len(df)
    batch_groesse = max(1, args.batch_groesse)
    gesamt_batches = math.ceil(gesamt_zeilen / batch_groesse)

    bereits_fertig = bereits_klassifizierte_batches_ermitteln(df, batch_groesse)
    logger.info(f"{gesamt_zeilen} Zeilen | {gesamt_batches} Batches | {len(bereits_fertig)} bereits klassifiziert")
    logger.info(f"Parallele API-Aufrufe: {args.parallelitaet}")

    fehler_anzahl = 0
    lock = threading.Lock()  # Schützt gleichzeitige Schreibzugriffe auf df und die CSV

    def batch_verarbeiten(batch_nr: int, start: int) -> None:
        """Verarbeitet einen einzelnen Batch — wird parallel ausgeführt."""
        nonlocal fehler_anzahl
        ende = min(start + batch_groesse, gesamt_zeilen)

        if batch_nr in bereits_fertig:
            logger.info(f"Batch {batch_nr + 1}/{gesamt_batches} | Zeilen {start}-{ende - 1} → bereits klassifiziert, wird übersprungen")
            return

        logger.info(f"Batch {batch_nr + 1}/{gesamt_batches} | Zeilen {start}-{ende - 1} → wird klassifiziert...")

        df_batch = df.iloc[start:ende].copy()
        nutzdaten = batch_nutzdaten_erstellen(df_batch, textspalten)

        try:
            batch_ergebnis = batch_klassifizieren(
                client=client,
                model=args.model,
                pdf_text=pdf_text,
                nutzer_prompt=prompt_text,
                zeilen_nutzdaten=nutzdaten,
                max_versuche=args.max_versuche,
                logger=logger,
            )
            with lock:
                ergebnisse_anwenden(df, batch_ergebnis, args.pruef_schwelle)
                zwischenspeichern(df, ausgabe_pfad)
            logger.info(f"Batch {batch_nr + 1}/{gesamt_batches} → Erfolgreich, Zwischenspeicherung abgeschlossen")

        except Exception as e:
            with lock:
                fehler_anzahl += 1
            logger.error(f"Batch {batch_nr + 1}/{gesamt_batches} → FEHLER: {e}")
            logger.error("Die Zeilen dieses Batches bleiben leer. Starten Sie das Skript erneut, um es erneut zu versuchen.")

    batches = list(enumerate(range(0, gesamt_zeilen, batch_groesse)))

    with ThreadPoolExecutor(max_workers=args.parallelitaet) as executor:
        futures = {executor.submit(batch_verarbeiten, batch_nr, start): batch_nr for batch_nr, start in batches}
        for future in as_completed(futures):
            future.result()  # Ausnahmen werden hier weitergeleitet

    # Automatischer Nachholversuch für fehlgeschlagene Batches (sequenziell, bis zu 3 Runden)
    for runde in range(1, 4):
        fehlende = bereits_klassifizierte_batches_ermitteln(df, batch_groesse)
        offene_batches = [(nr, start) for nr, start in batches if nr not in fehlende]
        if not offene_batches:
            break
        logger.warning(f"Nachholrunde {runde}/3: {len(offene_batches)} Batch(es) noch nicht klassifiziert → erneuter Versuch...")
        fehler_anzahl = 0
        for batch_nr, start in offene_batches:
            batch_verarbeiten(batch_nr, start)
        if fehler_anzahl == 0:
            break
        time.sleep(5 * runde)  # Kurze Pause vor der nächsten Runde

    # Endgültige Ausgabe — Confidence und NeedsReview werden nicht exportiert
    pruef_anzahl = (df["NeedsReview"] == "TRUE").sum() if "NeedsReview" in df.columns else 0
    df_export = df.drop(columns=["Confidence", "NeedsReview"], errors="ignore")
    df_export.to_csv(ausgabe_pfad, index=False, encoding="utf-8-sig")
    logger.info(f"Endgültige Ausgabe gespeichert: {ausgabe_pfad}")
    logger.info(f"Zeilen mit NeedsReview=TRUE (nur im Log): {pruef_anzahl}/{gesamt_zeilen}")
    if fehler_anzahl:
        logger.warning(f"{fehler_anzahl} Batch(es) fehlgeschlagen — Skript erneut starten, um sie zu verarbeiten.")
    logger.info("=== Programmende ===")


if __name__ == "__main__":
    main()
