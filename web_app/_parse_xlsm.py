import zipfile, xml.etree.ElementTree as ET, re
from pathlib import Path
xlsm = Path(r"C:\Users\misha\OneDrive\Рабочий стол\web_app\Отчет по позициям с неоплаченными.xlsm")
ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
with zipfile.ZipFile(xlsm) as z:
    sst = ET.fromstring(z.read("xl/sharedStrings.xml"))
    strings = ["".join((t.text or "") for t in si.findall(".//m:t", ns)) for si in sst.findall("m:si", ns)]
    sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))

def col_to_num(col):
    n = 0
    for c in col: n = n * 26 + (ord(c.upper()) - 64)
    return n

cells = {}
for c in sheet.findall(".//m:c", ns):
    ref = c.attrib.get("r")
    m = re.match(r"([A-Z]+)(\d+)", ref)
    if not m: continue
    col, row = col_to_num(m.group(1)), int(m.group(2))
    v = c.find("m:v", ns)
    if v is None: continue
    val = strings[int(v.text)] if c.attrib.get("t") == "s" else v.text
    cells[(row, col)] = val
for r in range(1, 30):
    vals = [cells.get((r,c)) for c in range(1, 15)]
    if any(v is not None for v in vals):
        print(r, vals)