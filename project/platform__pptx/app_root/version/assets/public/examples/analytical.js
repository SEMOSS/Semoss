// ANALYTICAL: evidence, interpretation and action, with editable native charts.
// All organizations and numbers below are fictional demonstration data. Replace
// them with supplied evidence; preserve units and denominators when adapting.
// Execute this complete IIFE through ExecuteNodeCode; use the requested filename/count.
(async () => {
  const path = require("path");
  const PptxGenJS = require("pptxgenjs");
  const deck = require(path.join(ROOT, ".claude/skills/pptx/scripts/deck.js"));
  const pres = deck.create({ PptxGenJS, title: "Meridian service review",
    theme: { palette: { bg: "FFFFFF", ink: "222134", muted: "5B596B", accent: "5645B8",
      accentSoft: "E4E1F0", series: ["5645B8", "A7A2BA"] },
      fonts: { heading: "Arial", body: "Arial" } } });
  const footnote = "Illustrative data only / fictional Meridian service team";

  // 1. State the conclusion, then give most of the space to evidence. The right
  // margin carries one interpretation rather than another equal-sized card.
  const share = pres.addSlide();
  deck.text(share, "MERIDIAN / QUARTERLY REVIEW", { x: 0.75, y: 0.55, w: 10, h: 0.3,
    fontSize: 12, bold: true, charSpacing: 1.6, color: "5645B8" });
  deck.text(share, "Routine requests moved\nto self-service.", {
    x: 0.75, y: 1.18, w: 11.6, h: 1.35, fontSize: 37, bold: true });
  deck.chart(share, { type: "column", x: 0.75, y: 3.02, w: 8.1, h: 3.55,
    categories: ["Q1", "Q2"], series: [
      { name: "Self-service", values: [36, 52] },
      { name: "Assisted", values: [64, 48] }
    ], options: { barGrouping: "stacked", dataLabelPosition: "ctr", showValue: true,
      dataLabelColor: "FFFFFF", dataLabelFormatCode: '0"%"', dataLabelFontSize: 16,
      valAxisMinVal: 0, valAxisMaxVal: 100, valAxisMajorUnit: 25,
      valAxisLabelFormatCode: '0"%"', showLegend: true, legendPos: "b",
      chartColors: ["5645B8", "5B596B"], catAxisLabelFontSize: 16 } });
  deck.callout(share, { x: 9.3, y: 3.1, w: 3.2, h: 2.55, value: "+16",
    label: "percentage points", caption: "Self-service share: 36% to 52%",
    fontSize: 64, labelSize: 19, captionSize: 17, color: "5645B8" });
  deck.text(share, footnote, { x: 0.75, y: 7.02, w: 11.8, h: 0.25,
    fontSize: 11, color: "5B596B" });
  share.addNotes("Fictional example. Each quarter totals 100%. Self-service share rises from 36% to 52%, a 16 percentage point change.");

  // 2. Use paired bars for like-for-like comparison and label the unit clearly.
  // Quiet axes and one emphasized series keep the numbers visually dominant.
  const response = pres.addSlide();
  deck.text(response, "Response times improved\nacross three channels.", {
    x: 0.75, y: 0.68, w: 11.8, h: 1.42, fontSize: 37, bold: true });
  deck.text(response, "Minutes to first response", { x: 0.78, y: 2.42, w: 7.5, h: 0.32,
    fontSize: 16, color: "5B596B" });
  deck.chart(response, { type: "bar", x: 0.7, y: 2.94, w: 8.25, h: 3.74,
    categories: ["Email", "Chat", "Web"], series: [
      { name: "Q1", values: [14.2, 5.6, 7.4] },
      { name: "Q2", values: [9.1, 3.8, 4.2] }
    ], options: { chartColors: ["A7A2BA", "5645B8"], showValue: true,
      dataLabelFormatCode: "0.0", dataLabelFontSize: 14,
      valAxisMinVal: 0, valAxisMaxVal: 16, valAxisMajorUnit: 4,
      showLegend: true, legendPos: "b", catAxisLabelFontSize: 15 } });
  deck.text(response, "5.1", { x: 9.38, y: 3.0, w: 3.1, h: 1.2,
    fontSize: 67, bold: true, color: "5645B8" });
  deck.text(response, "minutes faster", { x: 9.42, y: 4.28, w: 3.1, h: 0.42,
    fontSize: 21, bold: true });
  deck.text(response, "Email shows the largest absolute reduction in this example.", {
    x: 9.42, y: 5.0, w: 3.05, h: 1.55, fontSize: 18, color: "5B596B" });
  deck.text(response, footnote, { x: 0.75, y: 7.02, w: 11.8, h: 0.25,
    fontSize: 11, color: "5B596B" });
  response.addNotes("Fictional Q1/Q2 data in minutes: Email 14.2/9.1, Chat 5.6/3.8, Web 7.4/4.2. Email's absolute reduction is 5.1 minutes. These comparisons do not establish causality.");

  // 3. Change the composition for the decision. The actions are separate native
  // text objects, so the recipient can revise them without touching a chart.
  const action = pres.addSlide();
  action.background = { color: "28213E" };
  deck.text(action, "THE NEXT DECISION", { x: 0.75, y: 0.6, w: 8.7, h: 0.3,
    fontSize: 12, bold: true, charSpacing: 1.8, color: "C9C1EE" });
  deck.text(action, "Scale the workflow.\nKeep a human handoff.", {
    x: 0.75, y: 1.22, w: 11.9, h: 1.45, fontSize: 39, bold: true, color: "FFFFFF" });
  const actions = [
    ["01", "Expand to repeat requests.", "Start with the cases that already have clear answers."],
    ["02", "Review unresolved cases.", "Keep an explicit route to the service team."],
    ["03", "Track response time.", "Compare the next period using the same definitions."]
  ];
  actions.forEach(([number, title, detail], i) => {
    const y = 3.18 + i * 1.12;
    deck.text(action, number, { x: 0.78, y, w: 0.95, h: 0.62,
      fontSize: 30, color: "C9C1EE" });
    deck.text(action, title, { x: 2.02, y, w: 10.4, h: 0.44,
      fontSize: 23, bold: true, color: "FFFFFF" });
    deck.text(action, detail, { x: 2.04, y: y + 0.52, w: 10.3, h: 0.42,
      fontSize: 18, color: "D5CFE6" });
  });
  deck.text(action, footnote, { x: 0.75, y: 7.02, w: 11.8, h: 0.25,
    fontSize: 11, color: "D5CFE6" });
  action.addNotes("Illustrative actions for a fictional service team. Replace with decisions supported by the user's actual evidence.");

  const outPath = path.join(ROOT, "analytical-example.pptx");
  await deck.save(pres, outPath, { slides: 3 });
  return deck.validate(outPath, { slides: 3, strictCanvas: false });
})()
