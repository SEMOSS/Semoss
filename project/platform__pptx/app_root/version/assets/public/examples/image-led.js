// IMAGE-LED: a photographic essay with three different image/text compositions.
// Borrow scale, crop and placement; replace content, image, credit and filename
// for the user's brief. These three slides are an example, not a required count.
// Execute this complete IIFE through ExecuteNodeCode after loading the pptx skill.
(async () => {
  const path = require("path");
  const PptxGenJS = require("pptxgenjs");
  const sharp = require("sharp");
  const skill = path.join(ROOT, ".claude/skills/pptx");
  const deck = require(path.join(skill, "scripts/deck.js"));
  const photo = path.join(skill, "assets/earthrise.jpg");
  const source = "https://science.nasa.gov/resource/apollo-8s-iconic-earthrise/";
  const pres = deck.create({ PptxGenJS, title: "A change of perspective",
    theme: { palette: { bg: "FFFFFF", ink: "15212B", muted: "505D69", accent: "83B9DB" },
      fonts: { heading: "Arial", body: "Arial" } } });

  // 1. Let the photograph occupy most of the cover. Text sits in its own dark
  // region, so its readability does not depend on an arbitrary image overlay.
  const cover = pres.addSlide();
  cover.background = { color: "000000" };
  deck.image(cover, { path: photo, x: 5.75, y: 0, w: 7.5833, h: 7.5, fit: "cover" });
  deck.text(cover, "EARTHRISE / APOLLO 8", { x: 0.75, y: 0.7, w: 4.7, h: 0.3,
    fontSize: 12, bold: true, charSpacing: 2, color: "83B9DB" });
  deck.text(cover, "A change\nof perspective.", { x: 0.75, y: 2.1, w: 5.1, h: 2.1,
    fontSize: 45, bold: true, color: "FFFFFF", breakLine: false });
  deck.text(cover, "One photograph.\nA different sense of scale.", {
    x: 0.78, y: 4.6, w: 4.6, h: 1.0, fontSize: 23, color: "C8D8E3" });
  deck.text(cover, "Photo: NASA/Bill Anders  |  December 24, 1968", {
    x: 0.78, y: 6.93, w: 5.1, h: 0.3, fontSize: 11, color: "C8D8E3" });
  cover.addNotes("SEMOSS design example. Photograph source and historical date: " + source);

  // 2. Contain the complete square image, with an offset text column and a
  // visible source caption. A different image can need a different text position.
  const frame = pres.addSlide();
  deck.text(frame, "THE FRAME", { x: 0.75, y: 0.65, w: 5.4, h: 0.3,
    fontSize: 12, bold: true, charSpacing: 2, color: "505D69" });
  deck.image(frame, { path: photo, x: 0.75, y: 1.3, w: 5.5, h: 5.5, fit: "contain" });
  deck.text(frame, "Look past\nthe foreground.", { x: 7.15, y: 1.65, w: 5.3, h: 1.8,
    fontSize: 39, bold: true, color: "15212B" });
  deck.text(frame, "The lunar surface anchors the image.\nEarth appears small against the darkness.", {
    x: 7.18, y: 4.02, w: 4.95, h: 1.6, fontSize: 22, color: "505D69" });
  deck.text(frame, "Apollo 8 Earthrise photograph  |  NASA/Bill Anders", {
    x: 0.75, y: 7.0, w: 8.0, h: 0.25, fontSize: 11, color: "505D69" });
  frame.addNotes("SEMOSS example commentary on the visible photograph. Photo source: " + source);

  // 3. Crop deliberately around Earth AND the lunar horizon. This crop is for
  // this photograph; choose a focal region that suits any replacement image.
  const metadata = await sharp(photo).metadata();
  const cropHeight = Math.round(metadata.width * 9 / 16);
  const cropTop = Math.min(Math.round(metadata.height * 0.35), metadata.height - cropHeight);
  const panorama = await sharp(photo).extract({ left: 0, top: cropTop,
    width: metadata.width, height: cropHeight }).jpeg({ quality: 90 }).toBuffer();
  const closing = pres.addSlide();
  closing.background = { color: "000000" };
  deck.image(closing, { data: "image/jpeg;base64," + panorama.toString("base64"),
    x: 0, y: 0, w: 13.3333, h: 7.5, fit: "cover" });
  deck.text(closing, "Keep the wider view.", { x: 0.75, y: 0.65, w: 10.8, h: 0.85,
    fontSize: 42, bold: true, color: "FFFFFF" });
  deck.text(closing, "A single image can change\nthe scale of a story.", {
    x: 0.8, y: 1.9, w: 5.8, h: 1.05, fontSize: 23, color: "C8D8E3" });
  deck.text(closing, "Photo: NASA/Bill Anders  |  Cropped for this example", {
    x: 0.8, y: 7.0, w: 8.2, h: 0.25, fontSize: 11, color: "101820" });
  closing.addNotes("SEMOSS design example with a photographic crop; text remains editable. Photo source: " + source);

  const outPath = path.join(ROOT, "image-led-example.pptx");
  await deck.save(pres, outPath, { slides: 3 });
  return deck.validate(outPath, { slides: 3, strictCanvas: false });
})()
