import csv
import os


BAD_WORDS = [
    "ТАРИФНОЕ РУКОВОДСТВО",
    "ТАРИФНЫЕ РАССТОЯНИЯ",
    "АЛФАВИТНЫЙ СПИСОК",
    "ОБЩИЕ ПОЛОЖЕНИЯ",
    "ВВОДНЫЕ ПОЛОЖЕНИЯ",
]


def clean_cell(cell: str) -> str:
    if cell is None:
        return ""
    return cell.replace("\ufeff", "").strip().strip('"').strip()


def is_bad_row(cleaned_row: list[str]) -> bool:
    if not cleaned_row:
        return True

    joined = " ".join(cleaned_row).strip()
    if not joined:
        return True

    for bad in BAD_WORDS:
        if bad in joined:
            return True

    return False


def load_csv_folder(folder_path):
    data = []

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, file_name)

        with open(file_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)   # ВАЖНО: delimiter не указываем, тут запятая

            for row in reader:
                cleaned_row = [clean_cell(cell) for cell in row]

                # убираем хвостовые пустые ячейки
                while cleaned_row and cleaned_row[-1] == "":
                    cleaned_row.pop()

                if is_bad_row(cleaned_row):
                    continue

                data.append(cleaned_row)

    return data