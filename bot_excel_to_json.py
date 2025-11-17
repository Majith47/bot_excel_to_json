# bot_excel_to_json.py

import os
import io
import json
import math

import numpy as np
import pandas as pd
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ====== CONFIG ======
# Railway la BOT_TOKEN environment variable-la set pannunga
BOT_TOKEN = os.getenv("BOT_TOKEN", "8146026128:AAHlXRHGT1Cx0KaprJaTrXhyymjHT1z-4Pc")

# ====== HELPER FUNCTIONS (DATA CONVERSION) ======

RIVER_NAME_MAP = {
    "Stratford St A": "Stratford St Andrew",
}

def val_or_NR(x):
    """NaN / empty → 'NR', illati int/float value return."""
    if pd.isna(x):
        return "NR"
    try:
        f = float(x)
        if math.isfinite(f) and f.is_integer():
            return int(f)
        return f
    except Exception:
        return x


def build_main_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    NO3, PO4, P table (per year sheet) -> long format:
    columns: date, river, nitrate, phosphate, phosphorus
    """
    df = df.replace("-", np.nan)

    # Dates: first column, from row index 2 (row 0 = header, row 1 = NO3/PO4/P labels)
    date_col = pd.to_datetime(df.iloc[2:, 0], errors="coerce")

    records = []
    nrows, ncols = df.shape

    for col in range(1, ncols):
        # Row 1 la 'NO3' irukkara columns thaaan nitrate column
        if str(df.iat[1, col]).strip().upper() == "NO3":
            river = str(df.iat[0, col]).strip()
            if not river or river.lower() == "nan":
                continue

            col_no3 = col
            col_po4 = col + 1 if col + 1 < ncols else None
            col_p = col + 2 if col + 2 < ncols else None

            for row_idx, date in enumerate(date_col, start=2):
                if pd.isna(date):
                    continue

                nitrate = df.iat[row_idx, col_no3]
                po4 = df.iat[row_idx, col_po4] if col_po4 is not None else np.nan
                p = df.iat[row_idx, col_p] if col_p is not None else np.nan

                # no readings at all → skip
                if pd.isna(nitrate) and pd.isna(po4) and pd.isna(p):
                    continue

                records.append(
                    {
                        "date": date,
                        "river": river,
                        "nitrate": nitrate,
                        "phosphate": po4,
                        "phosphorus": p,
                    }
                )

    return pd.DataFrame(records)


def build_ecoli_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    E-Coli sheet -> long format:
    columns: date, river, ecoli
    """
    df = df.replace("-", np.nan)

    date_col = pd.to_datetime(df.iloc[1:, 0], errors="coerce")
    records = []
    nrows, ncols = df.shape

    for col in range(1, ncols):
        river = str(df.iat[0, col]).strip()
        if not river or river.lower() == "nan":
            continue

        for row_idx, date in enumerate(date_col, start=1):
            if pd.isna(date):
                continue

            val = df.iat[row_idx, col]
            if pd.isna(val):
                continue

            records.append(
                {
                    "date": date,
                    "river": river,
                    "ecoli": val,
                }
            )

    ecoli_df = pd.DataFrame(records)
    ecoli_df["river"] = ecoli_df["river"].replace(RIVER_NAME_MAP)
    return ecoli_df


def build_main_all_from_bytes(main_bytes: bytes) -> pd.DataFrame:
    """
    Main NO3/PO4/P workbook (2024 + 2025 sheets) -> long dataframe.
    """
    xls = pd.ExcelFile(io.BytesIO(main_bytes))
    frames = []

    for sheet in xls.sheet_names:
        # Only 2024 & 2025 sheets (NO3, PO4, P Table 2024/2025)
        if "2024" in sheet or "2025" in sheet:
            df = pd.read_excel(io.BytesIO(main_bytes), sheet_name=sheet)
            frames.append(build_main_long(df))

    if not frames:
        raise ValueError("No 2024/2025 sheets found in NO3/PO4/P workbook.")

    return pd.concat(frames, ignore_index=True)


def build_ecoli_all_from_bytes(ecoli_bytes: bytes) -> pd.DataFrame:
    """
    E-Coli workbook (2024 + 2025 sheets) -> long dataframe.
    """
    xls = pd.ExcelFile(io.BytesIO(ecoli_bytes))
    frames = []

    for sheet in xls.sheet_names:
        if "2024" in sheet or "2025" in sheet:
            df = pd.read_excel(io.BytesIO(ecoli_bytes), sheet_name=sheet)
            frames.append(build_ecoli_long(df))

    if not frames:
        raise ValueError("No 2024/2025 sheets found in E-Coli workbook.")

    return pd.concat(frames, ignore_index=True)


def classify_workbook(file_bytes: bytes) -> str:
    """
    Excel file main readings aa illa E-coli aa nu identify panna:
    - Sheet name la 'coli' irundha → E. coli
    - Illati → main readings
    """
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    joined = " ".join(xls.sheet_names).lower()
    if "coli" in joined:
        return "ecoli"
    return "main"


def convert_two_excel_bytes_to_json(file1_bytes: bytes, file2_bytes: bytes) -> str:
    """
    2 Excel bytes (order any) -> final JSON string
    structure: [{date, river, nitrate, phosphate, phosphorus, ecoli}, ...]
    """
    kind1 = classify_workbook(file1_bytes)
    kind2 = classify_workbook(file2_bytes)

    if kind1 == kind2:
        raise ValueError(
            "Could not distinguish main NO3/PO4/P workbook from E-Coli workbook. "
            "Please check the files."
        )

    if kind1 == "main":
        main_bytes, ecoli_bytes = file1_bytes, file2_bytes
    else:
        main_bytes, ecoli_bytes = file2_bytes, file1_bytes

    main_all = build_main_all_from_bytes(main_bytes)
    ecoli_all = build_ecoli_all_from_bytes(ecoli_bytes)

    merged_parts = []
    for river in main_all["river"].unique():
        main_r = main_all[main_all["river"] == river].sort_values("date")
        ecoli_r = ecoli_all[ecoli_all["river"] == river].sort_values("date")

        if ecoli_r.empty:
            tmp = main_r.copy()
            tmp["ecoli"] = np.nan
            merged_parts.append(tmp)
        else:
            merged_r = pd.merge_asof(
                main_r,
                ecoli_r[["date", "ecoli"]],
                on="date",
                direction="nearest",
            )
            merged_parts.append(merged_r)

    merged_all = pd.concat(merged_parts, ignore_index=True)
    merged_all.sort_values(["date", "river"], inplace=True)

    output = []
    for _, row in merged_all.iterrows():
        output.append(
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "river": row["river"],
                "nitrate": val_or_NR(row["nitrate"]),
                "phosphate": val_or_NR(row["phosphate"]),
                "phosphorus": val_or_NR(row["phosphorus"]),
                "ecoli": val_or_NR(row["ecoli"]),
            }
        )

    return json.dumps(output, indent=4)


# ====== TELEGRAM HANDLERS ======

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Vanakkam! 😊\n\n"
        "Ithu *Excel → JSON River Readings* automation bot.\n\n"
        "➡ 2 Excel files anuppunga:\n"
        "   1) NO3, PO4 & P Table (main readings)\n"
        "   2) E-Coli Table\n\n"
        "Order matter aagathu – bot automatically identify pannum.\n"
        "Files receive aana odane JSON file generate panni tharen ✅"
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document

    # Only Excel files (.xlsx / .xls)
    if not (
        doc.file_name.lower().endswith(".xlsx")
        or doc.file_name.lower().endswith(".xls")
    ):
        await update.message.reply_text(
            "Please send Excel files only (.xlsx or .xls) 🙂"
        )
        return

    file = await doc.get_file()
    file_bytes = await file.download_as_bytearray()

    files = context.user_data.get("files", [])
    files.append(file_bytes)
    context.user_data["files"] = files

    if len(files) == 1:
        await update.message.reply_text(
            "✅ First Excel file received.\n"
            "Innum 1 Excel file (NO3/PO4/P or E-Coli) anuppunga."
        )
    elif len(files) == 2:
        await update.message.reply_text("⏳ 2 files received. Converting to JSON...")

        try:
            json_str = convert_two_excel_bytes_to_json(files[0], files[1])

            json_bytes = io.BytesIO(json_str.encode("utf-8"))
            json_bytes.name = "readings.json"

            await update.message.reply_document(
                document=json_bytes,
                caption="✅ JSON conversion complete. "
                        "This matches the example structure (date, river, nitrate, phosphate, phosphorus, ecoli).",
            )
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Error while converting: {e}\n"
                "Please check that you uploaded:\n"
                "- One NO3/PO4/P table workbook\n"
                "- One E-Coli workbook (2024 & 2025 sheets)."
            )

        # Reset for next run
        context.user_data["files"] = []


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start - Restart the bot\n"
        "Just send the 2 Excel files (NO3/PO4/P + E-Coli) to get the JSON output."
    )


def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    app.run_polling()


if __name__ == "__main__":
    main()
