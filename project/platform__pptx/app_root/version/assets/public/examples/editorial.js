// EDITORIAL: strong type, open space and changing scale for a conceptual proposal.
// The library program is fictional. Borrow the composition and type contrast;
// adapt the message, palette, fonts, geometry and count to the actual request.
// This complete IIFE uses native text/shapes alongside the optional text helper.
(async () => {
  const path = require("path");
  const PptxGenJS = require("pptxgenjs");
  const deck = require(path.join(ROOT, ".claude/skills/pptx/scripts/deck.js"));
  const pres = deck.create({ PptxGenJS, title: "Room to begin again",
    theme: { palette: { bg: "FFFFFF", ink: "23251F", muted: "55594E", accent: "A83420" },
      fonts: { heading: "Cambria", body: "Arial" } } });

  // 1. The message itself is the visual. A large serif title, small sans-serif
  // context and generous empty space establish an identity without a card grid.
  const cover = pres.addSlide();
  cover.background = { color: "A83420" };
  deck.text(cover, "NEIGHBORHOOD LIBRARY / A CONCEPT PROPOSAL", {
    x: 0.82, y: 0.65, w: 11.8, h: 0.32, fontSize: 12,
    bold: true, charSpacing: 1.8, color: "FFFFFF" });
  cover.addText("Room to\nbegin again.", { x: 0.78, y: 1.82, w: 10.7, h: 2.55,
    fontFace: "Cambria", fontSize: 66, bold: false, color: "FFFFFF", margin: 0 });
  deck.text(cover, "A shared place for focus,\nconversation and discovery.", {
    x: 0.85, y: 5.27, w: 8.6, h: 1.1, fontSize: 24, color: "FFFFFF" });
  deck.text(cover, "Fictional program / design example", { x: 0.85, y: 7.02, w: 9, h: 0.25,
    fontSize: 11, color: "FFFFFF" });
  cover.addNotes("SEMOSS editorial design example. This neighborhood library program is fictional.");

  // 2. Shift to an asymmetric white spread: an oversized chapter number, a
  // spanning headline and two open columns. Nothing requires a panel around copy.
  const spaces = pres.addSlide();
  spaces.addText("01", { x: 0.72, y: 1.0, w: 2.22, h: 1.66,
    fontFace: "Cambria", fontSize: 84, color: "A83420", margin: 0 });
  spaces.addText("A shared place.\nDifferent kinds of space.", {
    x: 3.35, y: 0.8, w: 9.15, h: 1.65, fontFace: "Cambria", fontSize: 39,
    color: "23251F", margin: 0 });
  const columns = deck.grid({ x: 3.38, y: 3.23, w: 8.78, h: 2.68,
    columns: 2, rows: 1, gap: 0.65 });
  const copy = [
    { title: "Find your focus.", text: "A quiet seat, a good light and time to stay with an idea." },
    { title: "Meet a new voice.", text: "A shared table for a conversation, a reading or a small group." }
  ];
  columns.forEach((box, i) => {
    deck.text(spaces, copy[i].title, { x: box.x, y: box.y, w: box.w, h: 0.56,
      fontSize: 23, bold: true, color: "A83420" });
    deck.text(spaces, copy[i].text, { x: box.x, y: box.y + 0.9, w: box.w, h: 1.65,
      fontSize: 22, color: "55594E" });
  });
  deck.text(spaces, "One address. More than one way to belong.", {
    x: 3.38, y: 6.37, w: 8.8, h: 0.4, fontSize: 19, italic: true, color: "23251F" });
  deck.text(spaces, "Fictional program / design example", { x: 0.85, y: 7.02, w: 9, h: 0.25,
    fontSize: 11, color: "55594E" });
  spaces.addNotes("Fictional proposal. The example uses type and spacing to distinguish shared identity from two different uses of the space.");

  // 3. A large statement and a smaller vertical progression create a different
  // rhythm. The connector, markers and every label remain separately editable.
  const start = pres.addSlide();
  start.background = { color: "23251F" };
  deck.text(start, "START SMALL", { x: 0.85, y: 0.7, w: 5.0, h: 0.3,
    fontSize: 12, bold: true, charSpacing: 1.8, color: "E2DCCF" });
  start.addText("One room.\nA useful\nbeginning.", { x: 0.82, y: 1.6, w: 6.08, h: 3.02,
    fontFace: "Cambria", fontSize: 51, color: "FFFFFF", margin: 0 });
  deck.text(start, "Learn from the people\nwho use it.", { x: 0.87, y: 5.22, w: 5.8, h: 1.0,
    fontSize: 24, color: "E2DCCF" });
  start.addShape(pres.ShapeType.line, { x: 7.48, y: 2.2, w: 0, h: 3.36,
    line: { color: "8E9780", width: 1.5 } });
  const steps = [
    ["Observe", "Listen to the people who already visit."],
    ["Pilot", "Try one small change in one room."],
    ["Refine", "Keep what works and improve what does not."]
  ];
  steps.forEach(([title, detail], i) => {
    const y = 1.88 + i * 1.68;
    start.addShape(pres.ShapeType.ellipse, { x: 7.39, y: y + 0.24, w: 0.18, h: 0.18,
      fill: { color: "FFFFFF" }, line: { color: "FFFFFF", transparency: 100 } });
    deck.text(start, title, { x: 8.07, y, w: 4.32, h: 0.49,
      fontSize: 25, bold: true, color: "FFFFFF" });
    deck.text(start, detail, { x: 8.09, y: y + 0.64, w: 4.1, h: 0.81,
      fontSize: 19, color: "E2DCCF" });
  });
  deck.text(start, "Fictional program / design example", { x: 0.85, y: 7.02, w: 9, h: 0.25,
    fontSize: 11, color: "E2DCCF" });
  start.addNotes("Fictional proposal. Observe, Pilot and Refine are editable native slide objects, not a flattened diagram.");

  const outPath = path.join(ROOT, "editorial-example.pptx");
  await deck.save(pres, outPath, { slides: 3 });
  return deck.validate(outPath, { slides: 3, strictCanvas: false });
})()
