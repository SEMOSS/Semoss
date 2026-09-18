# Editing an existing PowerPoint

Use the staged input file in the room. PptxGenJS creates new presentations; it does
not import an existing deck. Keep the original design and unrelated content when
making a requested edit. Do not rebuild a deck through the JSON renderer for a
small change. The native design examples are for new slides/decks, not an import API.

Work in `ExecuteNodeCode`, using a single async IIFE with every declaration inside it.
The curated `jszip` can read and write the package. Use Node/file tools to inspect
the relevant XML, relationships, notes and embedded data. Save to the exact requested
path under `ROOT`; preserve the input when the request specifies a separate output.

Before changing content, identify the intended slides from the order in
`ppt/presentation.xml` and its relationships. A slide filename is not proof of its
display order. Inspect the input with the JS validator to distinguish existing
issues from those introduced by the edit.

- For text-only changes, preserve paragraph/run formatting, namespaces and XML
  escaping. Replace the intended text within its existing run(s); assigning an
  entire text frame can remove rich formatting. Match uniquely and verify the
  occurrence count before replacing anything.
- Keep one paragraph per list item and preserve inherited bullet properties. Use
  `xml:space="preserve"` when an XML text node needs leading or trailing spaces.
- Adding, deleting or moving slides also changes the presentation ID list, package
  relationships and content types. Slide copies can share chart, media, SmartArt
  and workbook parts; isolate a shared part before changing only one copy's data.
- Changing a chart may require updating both cached chart values and its embedded
  workbook. Preserve series labels, units, axis definitions and relationships.
- Replace images through their existing relationships when possible. Keep the
  intended crop and frame; check that a new image's aspect ratio still suits them.
- In a template, remove an unused item's complete group when appropriate, including
  its image and labels. Empty placeholder text alone can leave orphaned decoration.

The preserved [upstream guidance](upstream.md) contains more package details. Its
shell/Python helpers, thumbnail conversion and LibreOffice workflow are unavailable
through these agent tools. If that long reference is needed, use
`LoadSkill(skill_name="pptx/references/upstream.md", max_bytes=65536)` and honor any
continuation marker. Paths written as `scripts/...` there refer to the skill root.

Validate the edited file with the expected slide count and review the requested
changes, text fit estimates and preserved design. Fix new structural errors. Report
any material unresolved issue honestly; do not claim rendered visual inspection.
