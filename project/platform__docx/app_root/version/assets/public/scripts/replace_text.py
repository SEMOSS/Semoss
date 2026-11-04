"""Replace ordinary Word text while retaining untouched ZIP parts and run formatting."""
from pathlib import Path
from zipfile import ZipFile

from lxml import etree

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
XML_SPACE = "{http://www.w3.org/XML/1998/namespace}space"


def replace_text(source, output, old, new, *, expected=1, part="word/document.xml"):
    """Edit one XML part; refuse ambiguous matches, complex paragraphs, and overwrites.

    Replacement text takes the formatting of its first character. This makes normal
    edits, not revisions. Headers/footers may be selected by their inspected part name.
    """
    source, output = Path(source), Path(output)
    if not old or expected < 1:
        raise ValueError("Provide nonempty old text and a positive expected match count")
    if any(character in old + new for character in "\n\r\t"):
        raise ValueError("Use native Word elements for paragraphs, breaks, or tabs")
    if source.resolve() == output.resolve() or output.exists():
        raise ValueError("Choose a new output path; preserve the existing file")
    with ZipFile(source) as package:
        names = package.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate ZIP members are not supported")
        if sum(entry.file_size for entry in package.infolist()) > 256 * 1024 * 1024:
            raise ValueError("Document exceeds the 256 MiB uncompressed editing limit")
        if any(name.startswith("_xmlsignatures/") for name in names):
            raise ValueError("Editing invalidates this document's digital signature")
        if package.getinfo(part).file_size > 32 * 1024 * 1024:
            raise ValueError("Selected XML part exceeds the 32 MiB editing limit")
        tree = etree.fromstring(package.read(part), etree.XMLParser(
            resolve_entities=False, no_network=True, load_dtd=False))
        if tree.getroottree().docinfo.doctype:
            raise ValueError("Document type declarations are not supported")
        edits = []
        for paragraph in tree.iter(W + "p"):
            nodes = list(paragraph.iter(W + "t"))
            text = "".join(node.text or "" for node in nodes)
            count = text.count(old)
            if not count:
                continue
            # Fields, revisions, drawings, links, and anchors need their own edit semantics.
            if any(child.tag not in {W + "pPr", W + "r"} for child in paragraph):
                raise ValueError("Match intersects a complex paragraph; inspect its XML")
            if any(node.tag in {W + "ins", W + "del", W + "pPrChange", W + "rPrChange"}
                   for node in paragraph.iter()):
                raise ValueError("Match intersects existing revision markup")
            for run in paragraph.findall(W + "r"):
                if any(child.tag not in {W + "rPr", W + "t"} for child in run):
                    raise ValueError("Match intersects a non-text run; inspect its XML")
            if any(ancestor.tag in {W + "ins", W + "del", W + "sdt"}
                   for ancestor in paragraph.iterancestors()):
                raise ValueError("Match is inside a revision or content control")
            edits.append((nodes, text, count))
        total = sum(count for _, _, count in edits)
        if total != expected:
            raise ValueError(f"Expected {expected} matches, found {total}; no output written")
        for nodes, text, _ in edits:
            spans, offset = [], 0
            for node in nodes:
                length = len(node.text or "")
                spans.append((node, offset, offset + length))
                offset += length
            positions, start = [], 0
            while (start := text.find(old, start)) >= 0:
                positions.append(start)
                start += len(old)
            for start in reversed(positions):
                end = start + len(old)
                touched = [(node, a, b) for node, a, b in spans if a < end and b > start]
                for index, (node, a, b) in enumerate(touched):
                    value = node.text or ""
                    left, right = max(0, start - a), min(b - a, end - a)
                    node.text = value[:left] + (new if index == 0 else "") + value[right:]
                    if node.text.startswith(" ") or node.text.endswith(" "):
                        node.set(XML_SPACE, "preserve")
        xml = etree.tostring(tree, encoding="UTF-8", xml_declaration=True, standalone=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(output, "x") as result:
            result.comment = package.comment
            for entry in package.infolist():
                result.writestr(entry, xml if entry.filename == part else package.read(entry))
    return {"filePath": str(output), "replacements": total, "part": part}
