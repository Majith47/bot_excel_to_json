# bot_excel_to_json.py

import io
import json
import logging
from datetime import datetime

import pandas as pd
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ====== SETTINGS ======
BOT_TOKEN = "8146026128:AAHlXRHGT1Cx0KaprJaTrXhyymjHT1z-4Pc"  # BotFather la kidaikka koodiya token ah inge podu

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ====== CORE CONVERSION LOGIC ======
def convert_spreadsheets_to_json(file1_bytes: bytes, file2_bytes: bytes) -> str:
    """
    file1 = main readings spreadsheet
    file2 = E. coli readings spreadsheet
    """

    # Excel read
    df_main = pd.read_excel(io.BytesIO(file1_bytes))
    df_ecoli = pd.read_excel(io.BytesIO(file2_bytes))

    # 1) Dash / blank → "NR"
    for df in (df_main, df_ecoli):
        # '-' as missing value
        df.replace("-", pd.NA, inplace=True)
        # NaN → "NR"
        df.fillna("NR", inplace=True)

    # 2) Date columns convert (COLUMN NAME IMPORTANT)
    # >>> Un Excel la "Date" nu illa, "Sample Date" / "Date_Collected" nu irundha
    # >>> inga column name maathanum
    df_main["Date"] = pd.to_datetime(df_main["Date"])
    df_ecoli["Date"] = pd.to_datetime(df_ecoli["Date"])

    df_main = df_main.sort_values("Date")
    df_ecoli = df_ecoli.sort_values("Date")

    # 3) E. coli column name check
    # Assume: E. coli reading column name = "E_coli"
    # Ungaloda sheet la "E. coli" / "Ecoli" nu irundha, atha inga maathunga.
    if "E_coli" not in df_ecoli.columns:
        raise ValueError(
            "E_coli column not found in E. coli spreadsheet. "
            "Please update the column name in the code."
        )

    # 4) Main readings + nearest date E. coli reading merge pannrom
    merged = pd.merge_asof(
        df_main,
        df_ecoli[["Date", "E_coli"]],
        on="Date",
        direction="nearest",  # nearest date match
    )

    # Ippo df_main oda row kku nearest E. coli value attach aagidum.
    # Separate E. coli-only entries JSON la varaathu.

    # 5) Example JSON structure-ku map panna place
    # Ippa simple-a record-per-row JSON format use panren:
    records = merged.to_dict(orient="records")

    # Client example JSON la structure nested-a irundha,
    # inga custom structure build pannalaam.
    # Example:
    # custom_records = []
    # for row in records:
    #     item = {
    #         "date": row["Date"].strftime("%Y-%m-%d"),
    #         "location": row["Location"],
    #         "readings": {
    #             "ph": row["pH"],
    #             "turbidity": row["Turbidity"],
    #             "e_coli": row["E_coli"],
    #         }
    #     }
    #     custom_records.append(item)
    #
    # return json.dumps(custom_records, indent=4)

    return json.dumps(records, indent=4, default=str)


# ====== TELEGRAM HANDLERS ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Vanakkam! 😊\n\n"
        "Ithu Excel → JSON converter bot.\n"
        "1️⃣ Mudhal spreadsheet file (.xlsx) ah anuppunga.\n"
        "2️⃣ Athukkapparam 2nd spreadsheet file ah anuppunga.\n"
        "Files receive aana odane, JSON file create panni tharum ✅"
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document

    # Only Excel files consider pannuvom
    if not (document.file_name.endswith(".xlsx") or document.file_name.endswith(".xls")):
        await update.message.reply_text(
            "Please Excel spreadsheet (.xlsx / .xls) file dhaan anuppunga 🙂"
        )
        return

    file = await document.get_file()
    file_bytes = await file.download_as_bytearray()

    files = context.user_data.get("files", [])
    files.append(file_bytes)
    context.user_data["files"] = files

    if len(files) == 1:
        await update.message.reply_text(
            "✅ First spreadsheet receive aayiduchu.\n"
            "Ippo 2nd spreadsheet file ah anuppunga."
        )
    elif len(files) == 2:
        await update.message.reply_text("⏳ 2 files receive aayiduchu, JSON create panren...")

        try:
            json_str = convert_spreadsheets_to_json(files[0], files[1])

            # JSON as file send pannuvom
            json_bytes = io.BytesIO(json_str.encode("utf-8"))
            json_bytes.name = "output.json"

            await update.message.reply_document(
                document=json_bytes,
                caption="✅ JSON file ready! Idha Upwork client kku upload pannalum. 👍",
            )
        except Exception as e:
            logger.exception("Conversion error:")
            await update.message.reply_text(
                f"⚠️ Error: {e}\n\n"
                "Column names / JSON structure la konjam adjust panna venum pola irukku."
            )

        # Reset files for next run
        context.user_data["files"] = []


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start - bot restart pannum\n"
        "Simply 2 Excel spreadsheets anuppi JSON vaangalaam 🙂"
    )


def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    app.run_polling()


if __name__ == "__main__":
    main()
