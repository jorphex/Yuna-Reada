import io
import xml.etree.ElementTree as ET


def build_opml_tree(feeds):
    root = ET.Element("opml")
    body = ET.SubElement(root, "body")
    outline_parent = ET.SubElement(body, "outline")
    for feed in feeds:
        outline = ET.SubElement(outline_parent, "outline")
        text = feed.get("text") or feed.get("xmlUrl") or "Unknown Feed"
        outline.set("text", str(text))
        outline.set("xmlUrl", str(feed.get("xmlUrl", "")))
    indent(root)
    return ET.ElementTree(root)


def build_opml_bytes(feeds) -> bytes:
    buffer = io.BytesIO()
    build_opml_tree(feeds).write(buffer, encoding="utf-8", xml_declaration=True)
    return buffer.getvalue()


def write_opml(feeds, filename):
    build_opml_tree(feeds).write(filename, encoding="utf-8", xml_declaration=True)


async def send_opml(bot, chat_id: int, feeds):
    document = io.BytesIO(build_opml_bytes(feeds))
    document.name = f"yunareada-{chat_id}-feeds.opml"
    await bot.send_document(chat_id=chat_id, document=document, filename=document.name)


def indent(elem, level=0):
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for child in elem:
            indent(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    elif level and (not elem.tail or not elem.tail.strip()):
        elem.tail = i
