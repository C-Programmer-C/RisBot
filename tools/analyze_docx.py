import re
import zipfile
from pathlib import Path


def read_document_xml(docx_path: Path) -> str:
    with zipfile.ZipFile(docx_path) as z:
        return z.read("word/document.xml").decode("utf-8")


def summarize(docx_path: Path) -> None:
    xml = read_document_xml(docx_path)
    cnt_gen = xml.count("Генеральный директор")
    cnt_dir = xml.count("Директор")
    cnt_mp = xml.count("М.П.")
    print(f"\n== {docx_path} ==")
    print(f"Генеральный директор: {cnt_gen} | Директор: {cnt_dir} | М.П.: {cnt_mp}")

    print(f"<w:tbl occurrences: {xml.count('<w:tbl')}")

    # Correct: [\\s\\S] in raw string matches any char including newlines
    tables = re.findall(r"<w:tbl[\s\S]*?</w:tbl>", xml)
    print(f"tables: {len(tables)}")
    if not tables:
        return

    last = tables[-1]
    texts = re.findall(r"<w:t[^>]*>(.*?)</w:t>", last)
    text = "".join(
        t.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">") for t in texts
    )
    tnorm = re.sub(r"\\s+", " ", text).strip()

    m = re.search(r"(Генеральный директор.{0,220})", tnorm)
    print("last table snippet:", m.group(1) if m else "NO_DIRECTOR_IN_LAST_TABLE")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    template = root / "Шаблон.docx"
    output = root / "data" / "output_330974803.docx"

    for p in (template, output):
        if not p.exists():
            print("missing", p)
            continue
        summarize(p)


if __name__ == "__main__":
    main()

