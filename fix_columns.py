import pandas as pd
import sys
import os

# Target column order (from MFA_AR_260313.csv)
TARGET_COLUMNS = [
    "FrageNr", "lfdNr", "BerufNr", "Beruf",
    "Abschlussprüfung Teil 1", "Abschlussprüfung Teil 2",
    "Lehrjahr", "LFNr", "LF", "AbschnNr", "Abschnitt",
    "Nr", "Frage", "A", "B", "C", "D",
    "Richtig1", "Richtig_Text1", "Bild", "Sprache"
]

ANSWER_LETTERS = {"A", "B", "C", "D"}


def read_csv(path):
    return pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str)


def fix_columns(input_path: str, ref_df: pd.DataFrame = None) -> str:
    df = read_csv(input_path).reset_index(drop=True)

    # Normalize: rename Richtig -> Richtig1 if present
    if "Richtig" in df.columns and "Richtig1" not in df.columns:
        df = df.rename(columns={"Richtig": "Richtig1"})

    # If Richtig1 is still missing, take it from the reference file (by row order)
    if "Richtig1" not in df.columns:
        if ref_df is None:
            df["Richtig1"] = ""
        else:
            ref = ref_df.reset_index(drop=True)
            if "Richtig" in ref.columns and "Richtig1" not in ref.columns:
                ref = ref.rename(columns={"Richtig": "Richtig1"})
            df["Richtig1"] = ref["Richtig1"] if "Richtig1" in ref.columns else ""

    # Always derive Richtig_Text1 from the letter in Richtig1 and the A/B/C/D columns of THIS file
    def get_richtig_text(row):
        letter = str(row.get("Richtig1", "")).strip()
        if letter in ANSWER_LETTERS and letter in row:
            return row[letter]
        return ""

    df["Richtig_Text1"] = df.apply(get_richtig_text, axis=1)

    # Build output dataframe with target columns in correct order
    out = pd.DataFrame(columns=TARGET_COLUMNS)
    for col in TARGET_COLUMNS:
        if col in df.columns:
            out[col] = df[col].values
        else:
            out[col] = ""

    # Output file name: original name + _fixed_column
    base, ext = os.path.splitext(input_path)
    output_path = f"{base}_fixed_column{ext}"

    out.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"  -> {output_path}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python fix_columns.py <file1.csv> [file2.csv ...]")
        print("  python fix_columns.py --reference ref.csv <file1.csv> [file2.csv ...]")
        sys.exit(1)

    args = sys.argv[1:]
    ref_df = None
    ref_path = None

    if "--reference" in args:
        idx = args.index("--reference")
        ref_path = args[idx + 1]
        args = args[:idx] + args[idx + 2:]
        if not os.path.isfile(ref_path):
            print(f"[ERROR] Reference file not found: {ref_path}")
            sys.exit(1)
        ref_df = read_csv(ref_path)
        print(f"Reference file: {ref_path}")

    # If no input files given but a reference was provided, process the reference itself
    if not args and ref_path:
        args = [ref_path]

    for path in args:
        if not os.path.isfile(path):
            print(f"[SKIP] Not found: {path}")
            continue
        print(f"Processing: {path}")
        fix_columns(path, ref_df=ref_df)

    print("Done.")
