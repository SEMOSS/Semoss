'use strict';

// Run from the repository root: node --test js/test/pptx_deck.test.js
// Uses the curated node_env, with no additional test dependencies.
const { test, before } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createRequire } = require('node:module');
const { spawnSync } = require('node:child_process');
const envRequire = createRequire(path.resolve(__dirname, '../node_env/package.json'));
const PptxGenJS = envRequire('pptxgenjs');
const JSZip = envRequire('jszip');
const skill = path.resolve(__dirname, '../../project/platform__pptx/app_root/version/assets/public');
const modulePath = path.join(skill, 'scripts/deck.js');
const deck = require(modulePath);
const output = process.env.PPTX_TEST_OUTPUT || fs.mkdtempSync(path.join(os.tmpdir(), 'semoss-pptx-deck-'));
fs.mkdirSync(output, { recursive: true });
const file = name => path.join(output, name + '.pptx');
const pixel = 'image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1cAAAAASUVORK5CYII=';

function frozen(value) {
  if (value && typeof value === 'object') { Object.values(value).forEach(frozen); Object.freeze(value); }
  return value;
}
async function parts(filename) { return JSZip.loadAsync(fs.readFileSync(filename)); }
async function xml(filename, part = 'ppt/slides/slide1.xml') { return (await parts(filename)).file(part).async('string'); }
async function chartXml(filename) { return (await parts(filename)).file(/^ppt\/charts\/chart\d+\.xml$/)[0].async('string'); }
function valid(filename, slides, options) {
  const result = deck.validate(filename, { slides, ...options });
  assert.equal(result.ok, true, JSON.stringify(result));
  return result;
}
async function mutate(name, part, edit, source = file('native-five-slides')) {
  const zip = await parts(source);
  const old = await zip.file(part).async('string');
  const changed = edit(old);
  assert.notEqual(changed, old);
  zip.file(part, changed);
  fs.writeFileSync(file(name), await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' }));
  return file(name);
}
function create(options) { return deck.create({ PptxGenJS, ...options }); }

test('cover accepts color strings and native background objects without changing caller options', async () => {
  const pres = create();
  for (const background of ['1A7F5A', frozen({ color: '1A7F5A' })]) {
    deck.cover(pres.addSlide(), frozen({ title: 'Garbage', background }));
  }
  await deck.save(pres, file('cover-background-options'));
  valid(file('cover-background-options'), 2);
  for (const index of [1, 2]) {
    const slide = await xml(file('cover-background-options'), `ppt/slides/slide${index}.xml`);
    assert.match(slide, /<p:bg>.*?<a:srgbClr val="1A7F5A"/);
  }
});

test('cover rejects malformed background values at the component boundary', () => {
  const slide = create().addSlide();
  for (const background of [{ color: { color: '1A7F5A' } }, [], 12, null, {}]) {
    assert.throws(() => deck.cover(slide, { title: 'Garbage', background }), /deck.cover background must be a color string/);
  }
});

test('nested component geometry changes the saved callout caption box', async () => {
  const pres = create();
  const options = frozen({ value: '2.0', label: 'Million years', caption: 'A caption needing room',
    geometry: { x: 0.95, y: 2, w: 11.4, h: 5 }, x: 1, captionSize: 16 });
  deck.callout(pres.addSlide(), options);
  await deck.save(pres, file('nested-callout'));
  const shapes = (await xml(file('nested-callout'))).match(/<p:sp>.*?<\/p:sp>/g);
  const caption = shapes.find(shape => shape.includes('A caption needing room'));
  assert.match(caption, /<a:off x="914400" y="5257800"/);
  assert.match(caption, /<a:ext cx="10424160" cy="1143000"/);
  assert.equal(options.geometry.x, 0.95);
  assert.equal(options.geometry.h, 5);
});

test('malformed nested geometry fails with a useful correction', () => {
  const slide = create().addSlide();
  for (const geometry of [{ width: 11.4 }, [], 'large', null]) {
    assert.throws(() => deck.callout(slide, { value: '2', geometry }), /geometry.*x, y, w and h/);
  }
});

before(async () => {
  const pres = create({ title: 'Regional orientation', theme: { preset: 'forest', fonts: { heading: 'Cambria', body: 'Arial' } } });
  deck.cover(pres.addSlide(), { kicker: 'Regional orientation', title: 'A region,\nthree jurisdictions',
    subtitle: 'Washington, D.C., Maryland and Virginia' });
  const comparison = pres.addSlide();
  deck.heading(comparison, { title: 'Across state and district lines' });
  deck.callout(comparison, { x: 0.8, y: 2.1, w: 3.2, h: 3, value: '3', label: 'Jurisdictions',
    caption: 'A federal district and parts of two states' });
  deck.comparison(comparison, { x: 4.8, y: 2.1, w: 7.5, h: 3.9,
    left: { heading: 'Maryland', text: 'State capital: Annapolis' }, right: { heading: 'Virginia', text: 'State capital: Richmond' } });
  const processSlide = pres.addSlide();
  deck.heading(processSlide, { title: 'Planning a regional visit' });
  deck.timeline(processSlide, { y: 2.2, h: 3.8, steps: [
    { title: 'Choose a destination', text: 'Confirm which jurisdiction you are visiting.' },
    { title: 'Check the route', text: 'Review the stops and transfers for your trip.' },
    { title: 'Allow enough time', text: 'Check the current schedule before leaving.' }
  ] });
  const chart = pres.addSlide();
  deck.heading(chart, { title: 'Jurisdiction types' });
  deck.chart(chart, { type: 'bar', x: 0.8, y: 2, w: 7, h: 4.4, categories: ['Federal district', 'States'],
    series: [{ name: 'Count', values: [1, 2] }], options: { valAxisMaxVal: 2, valAxisMajorUnit: 1 } });
  deck.text(chart, 'The region crosses administrative boundaries.', { x: 8.9, y: 2.6, w: 3.4, h: 2, fontSize: 27 });
  const custom = pres.addSlide();
  custom.background = { color: '#14301F' };
  custom.addText('Start with a place,\nthen plan the journey.', { x: 0.9, y: 2, w: 11, h: 2.2,
    fontFace: 'Cambria', fontSize: 48, color: '#FFFFFF' });
  custom.addNotes('Demonstration content. Check current travel schedules before use.');
  await deck.save(pres, file('native-five-slides'), { slides: 5 });
});

test('five-slide deck combines editable components with a custom native slide', async () => {
  const result = valid(file('native-five-slides'), 5);
  assert.deepEqual(result.warnings, []);
  assert.equal(result.review.rendered, false);
  const zip = await parts(file('native-five-slides'));
  assert.equal(zip.file(/^ppt\/charts\/chart\d+\.xml$/).length, 1);
  assert.equal(zip.file(/^ppt\/embeddings\/.*\.xlsx$/).length, 1);
  assert.match(await xml(file('native-five-slides'), 'ppt/slides/slide3.xml'), /timeline connector/);
  assert.match(await xml(file('native-five-slides'), 'ppt/slides/slide5.xml'), /Start with a place/);
  assert.match(await xml(file('native-five-slides'), 'ppt/notesSlides/notesSlide5.xml'), /Demonstration content/);
});

test('frozen native options can be reused without mutation and signed geometry retains direction', async () => {
  const pres = create();
  const slide = pres.addSlide();
  const options = frozen({ x: 4, y: 3, w: -2, h: -1, fill: { color: '#ab1234' },
    line: { color: '#123abc' }, shadow: { type: 'outer', color: '#000000', offset: -2, angle: 90, opacity: 0.2 } });
  slide.addShape(pres.ShapeType.rect, options);
  slide.addShape(pres.ShapeType.rect, options);
  await deck.save(pres, file('safe-options'));
  valid(file('safe-options'), 1);
  const outputXml = await xml(file('safe-options'));
  assert.match(outputXml, /flipH="1"/);
  assert.match(outputXml, /flipV="1"/);
  assert.match(outputXml, /<a:off x="1828800" y="1828800"/);
  assert.equal((outputXml.match(/<a:srgbClr val="AB1234"/g) || []).length, 2);
  assert.equal(options.w, -2);
  assert.equal(options.shadow.offset, -2);
});

test('native rich text, hyperlinks, tables and masters remain usable', async () => {
  const pres = create();
  assert.ok(pres instanceof PptxGenJS);
  pres.defineSlideMaster({ title: 'CUSTOM', background: { color: '112233' }, objects: [
    { text: { text: 'Master footer', options: { x: 0.6, y: 7, w: 3, h: 0.3, fontSize: 12, color: 'FFFFFF' } } }
  ] });
  const slide = pres.addSlide({ masterName: 'CUSTOM' });
  const runs = frozen([{ text: 'Rich ', options: { color: '#ff0000', bold: true } },
    { text: 'text', options: { color: '#00ff00', charSpacing: 2, hyperlink: { url: 'https://example.com' } } }]);
  slide.addText(runs, { x: 1, y: 1, w: 8, h: 1, fontSize: 30 });
  slide.addTable(frozen([['Name', 'Value'], ['Item', '42']]), { x: 1, y: 3, w: 8, h: 2,
    fontSize: 20, color: '#ffffff', fill: { color: '#112233' }, border: { color: '#445566', pt: 1 } });
  await deck.save(pres, file('native-api'));
  valid(file('native-api'), 1);
  const outputXml = await xml(file('native-api'));
  assert.match(outputXml, /<a:tbl>/);
  assert.match(outputXml, /<a:hlinkClick/);
  assert.match(outputXml, /spc="200"/);
  assert.doesNotMatch(outputXml, /<p:bg>/, 'master background should be inherited');
  assert.equal(runs[0].options.color, '#ff0000');
});

test('component images use installed PptxGenJS cover and contain sizing', async () => {
  const pres = create();
  const slide = pres.addSlide();
  const imageOptions = frozen({ data: pixel, x: 1, y: 1, w: 4, h: 2, flipH: true });
  deck.image(slide, { ...imageOptions, fit: 'cover' });
  deck.image(slide, { ...imageOptions, x: 6, fit: 'contain' });
  deck.cover(pres.addSlide(), { title: 'Photo composition', subtitle: 'A test of native image cropping', image: { data: pixel } });
  await deck.save(pres, file('images'));
  valid(file('images'), 2);
  const outputXml = await xml(file('images'));
  assert.equal((outputXml.match(/<p:pic>/g) || []).length, 2);
  assert.match(outputXml, /<a:srcRect[^>]*t="25000"/);
  assert.match(outputXml, /<a:srcRect[^>]*l="-50000"/);
  assert.equal(imageOptions.w, 4);
});

test('components accept custom geometry and native chart styling without dropping data', async () => {
  const pres = create({ width: 10, height: 6, theme: { palette: { accent: '#B14D32' }, fonts: { heading: 'Cambria', body: 'Arial' } } });
  const slide = pres.addSlide();
  deck.heading(slide, { title: 'Custom canvas', x: 0.4, y: 0.3, w: 9, h: 0.7, fontSize: 32 });
  deck.chart(slide, { type: 'column', x: 0.5, y: 1.5, w: 9, h: 3.8, categories: ['A', 'B'],
    series: [{ name: 'Count', values: [10, 20] }], options: { barGrouping: 'stacked', dataLabelPosition: 'outEnd',
      chartColors: ['#b14d32'], valAxisMaxVal: 25, valAxisMajorUnit: 5 } });
  await deck.save(pres, file('custom-canvas'));
  valid(file('custom-canvas'), 1);
  const outputXml = await chartXml(file('custom-canvas'));
  assert.match(outputXml, /<c:dLblPos val="inEnd"/);
  assert.match(outputXml, /<c:max val="25"/);
  assert.match(outputXml, /<c:v>20<\/c:v>/);
  assert.match(outputXml, /B14D32/);
  assert.throws(() => deck.addLayout(pres, { kind: 'title', title: 'Wrong canvas' }), /custom canvas/);
});

test('JPEG image dimensions produce real cropping instead of stretching', async () => {
  const sharp = envRequire('sharp');
  const buffer = await sharp({ create: { width: 64, height: 16, channels: 3, background: '#336699' } }).jpeg().toBuffer();
  const source = path.join(output, 'wide-image.jpg');
  fs.writeFileSync(source, buffer);
  const pres = create();
  deck.image(pres.addSlide(), { path: source, x: 1, y: 1, w: 4, h: 2, fit: 'cover' });
  await deck.save(pres, file('jpeg-crop'));
  valid(file('jpeg-crop'), 1);
  assert.match(await xml(file('jpeg-crop')), /<a:srcRect l="25000" r="25000" t="0" b="0"/);
});

test('native combo charts preserve per-series options and normalize stacked labels', async () => {
  const pres = create();
  const data = [{ name: 'One', labels: ['A', 'B'], values: [1, 2] }];
  const combos = frozen([
    { type: pres.ChartType.bar, data, options: { barGrouping: 'stacked', dataLabelPosition: 'outEnd', showValue: true } },
    { type: pres.ChartType.line, data: [{ name: 'Two', labels: ['A', 'B'], values: [2, 3] }], options: { showValue: true, dataLabelPosition: 't' } }
  ]);
  const options = frozen({ x: 1, y: 1, w: 10, h: 5 });
  pres.addSlide().addChart(combos, options);
  await deck.save(pres, file('combo'));
  valid(file('combo'), 1);
  const outputXml = await chartXml(file('combo'));
  assert.match(outputXml, /<c:barChart>/);
  assert.match(outputXml, /<c:lineChart>/);
  assert.match(outputXml, /<c:dLblPos val="inEnd"/);
  assert.match(outputXml, /<c:dLblPos val="t"/);
  assert.match(await xml(file('combo')), /<a:ext cx="9144000" cy="4572000"/);
  assert.equal(combos[0].options.dataLabelPosition, 'outEnd');
});

test('native stacked charts receive the same protection as component charts', async () => {
  const pres = create();
  const slide = pres.addSlide();
  const data = frozen([{ name: 'A', labels: ['One', 'Two'], values: [2, 4] }]);
  const options = frozen({ x: 1, y: 1, w: 10, h: 5, barGrouping: 'percentStacked', showValue: true, dataLabelPosition: 'outEnd' });
  slide.addChart(pres.ChartType.bar, data, options);
  await deck.save(pres, file('native-stacked'));
  valid(file('native-stacked'), 1);
  assert.equal(options.dataLabelPosition, 'outEnd');
  assert.match(await chartXml(file('native-stacked')), /<c:dLblPos val="inEnd"/);
});

test('bullets normalize glyphs and native paragraphs keep their formatting', async () => {
  const pres = create();
  const slide = pres.addSlide();
  deck.bullets(slide, ['\u2022 First', { text: '- Second', level: 1, bold: true }], { x: 1, y: 1, w: 9, h: 3 });
  slide.addText(frozen([{ text: '\u2022 Third', options: { bullet: true } }]), { x: 1, y: 5, w: 9, h: 1, fontSize: 20 });
  await deck.save(pres, file('bullets'));
  valid(file('bullets'), 1);
  const outputXml = await xml(file('bullets'));
  assert.equal((outputXml.match(/<a:buChar/g) || []).length, 3);
  assert.doesNotMatch(outputXml, /<a:t>[^<]*\u2022/);
  assert.match(outputXml, /<a:t>Second<\/a:t>/);
});

test('legacy JSON layout can be customized before saving with native slides', async () => {
  const pres = create();
  const slide = deck.addLayout(pres, { kind: 'bullets', title: 'A starting point', bullets: ['A supplied detail'] });
  slide.addText('Custom annotation', { x: 8, y: 5, w: 4, h: 0.6, color: '#336699', fontSize: 25 });
  deck.cover(pres.addSlide(), { title: 'A separate composition' });
  await deck.save(pres, file('mixed-layouts'));
  valid(file('mixed-layouts'), 2);
  const outputXml = await xml(file('mixed-layouts'));
  assert.match(outputXml, /Custom annotation/);
  assert.doesNotMatch(outputXml, /undefined/);
});

test('grid supports arbitrary row/column compositions and percentage positions', async () => {
  const pres = create({ width: 10, height: 6 });
  const slide = pres.addSlide();
  const cells = deck.grid({ x: 0.5, y: 1, w: 9, h: 4, rows: 2, columns: 3, gap: 0.3 });
  cells.forEach((box, i) => slide.addText(String(i + 1), { ...box, fontSize: 32 }));
  slide.addShape(pres.ShapeType.line, { x: '5%', y: '95%', w: '90%', h: 0, line: { color: '#123456' } });
  await deck.save(pres, file('grid'));
  valid(file('grid'), 1);
  assert.match(await xml(file('grid')), /<a:off x="457200" y="5212080"/);
  assert.throws(() => deck.grid({ columns: 20, w: 1, gap: 1 }), /no space/);
});

test('count metadata survives another process and mismatched saves preserve existing files', async () => {
  const script = 'process.stdout.write(JSON.stringify(require(' + JSON.stringify(modulePath) + ').validate(' + JSON.stringify(file('native-five-slides')) + ')))';
  const run = spawnSync(process.execPath, ['-e', script], { encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr);
  assert.equal(JSON.parse(run.stdout).expectedSlides, 5);
  const pres = create();
  pres.addSlide();
  fs.writeFileSync(file('preserve-output'), 'original');
  await assert.rejects(deck.save(pres, file('preserve-output'), { slides: 5 }), /match the requested count/);
  assert.equal(fs.readFileSync(file('preserve-output'), 'utf8'), 'original');
  await assert.rejects(deck.save(pres, 'relative.pptx'), /absolute/);
});

test('repeated saving updates native slide counts and preserves user metadata', async () => {
  const pres = create();
  pres.addSlide();
  await deck.save(pres, file('resaved'));
  pres.addSlide();
  await deck.save(pres, file('resaved'));
  assert.equal(valid(file('resaved'), 2).expectedSlides, 2);
  pres.subject = 'User-supplied subject';
  await deck.save(pres, file('custom-subject'));
  assert.match(await xml(file('custom-subject'), 'docProps/core.xml'), /User-supplied subject/);
});

test('layout review reports actionable warnings without declaring a valid package corrupt', async () => {
  const pres = create();
  const slide = pres.addSlide();
  slide.addText('Long descriptive text that will not fit. '.repeat(12), { x: 1, y: 1, w: 2, h: 0.2, fontSize: 11, color: 'EEEEEE', objectName: 'crowded copy' });
  slide.addText('Overlapping text', { x: 1, y: 1, w: 2, h: 0.2, fontSize: 18, objectName: 'overlap' });
  slide.addShape(pres.ShapeType.ellipse, { x: 12, y: 4, w: 3, h: 3, fill: { color: '112233' }, objectName: 'intentional bleed' });
  await deck.save(pres, file('layout-warnings'));
  const result = valid(file('layout-warnings'), 1, { strictCanvas: false });
  for (const id of ['off-canvas', 'small-text', 'estimated-text-overflow', 'possible-text-overlap', 'low-contrast']) {
    assert.ok(result.diagnostics.some(item => item.id === id && item.slide === 1), id);
  }
  assert.ok(result.diagnostics.every(item => item.severity === 'warning' && item.object && item.message));
  assert.equal(deck.validate(file('layout-warnings')).ok, false, 'strict canvas remains backward compatible');
});

test('corrupt extents and colors still fail when decorative bleed is allowed', async () => {
  const negative = await mutate('negative', 'ppt/slides/slide1.xml', value => value.replace(/<a:ext cx="\d+" cy="\d+"\/>/, '<a:ext cx="-1" cy="200"/>'));
  const alpha = await mutate('alpha', 'ppt/slides/slide1.xml', value => value.replace(/<a:srgbClr val="[A-F0-9]+"/, '<a:srgbClr val="FFFFFF80"'));
  for (const [filename, id] of [[negative, 'extents'], [alpha, 'colors']]) {
    const result = deck.validate(filename, { strictCanvas: false });
    assert.equal(result.ok, false);
    assert.ok(result.checks.some(check => check.id === id && !check.ok));
  }
  const bytes = fs.readFileSync(file('native-five-slides'));
  fs.writeFileSync(file('truncated'), bytes.subarray(0, bytes.length - 15));
  assert.equal(deck.validate(file('truncated'), { strictCanvas: false }).ok, false);
});

test('unsupported values fail at the helper boundary instead of dropping required content', () => {
  const pres = create();
  const slide = pres.addSlide();
  assert.throws(() => slide.addText('Text', { color: '#FFFFFF88' }), /Invalid color/);
  assert.throws(() => slide.addShape(undefined, {}), /instance ShapeType/);
  assert.throws(() => slide.addText('Text', { x: NaN }), /finite number/);
  assert.throws(() => deck.image(slide, {}), /path or data/);
  assert.throws(() => deck.timeline(slide, { steps: [{}] }), /title/);
  assert.throws(() => deck.chart(slide, { categories: ['A'], series: [{ values: [1, 2] }] }), /one finite number/);
});

test('optional JSON render remains available with the original spec schema', async () => {
  await deck.render({ PptxGenJS, outPath: file('legacy-json'), spec: frozen({ title: 'A simple deck', slides: [
    { kind: 'title', title: 'A simple deck' }, { kind: 'two-col', title: 'Comparison',
      left: { heading: 'Left', bullets: ['First'] }, right: { heading: 'Right', text: 'Second' } }
  ] }) });
  valid(file('legacy-json'), 2);
});


test('timeline marker contrast and separate text colors survive native PPTX export', async () => {
  const pres = create({ theme: { preset: 'dark', bg: '101820', ink: 'FFFFFF', muted: 'CBD5E1' } });
  deck.timeline(pres.addSlide(), { color: '38BDF8', steps: [{ label: 'Now', title: 'Discover', text: 'Details' }] });
  deck.timeline(pres.addSlide(), { color: '111111', markerTextColor: 'FFFF00', labelColor: '00FFFF',
    titleColor: 'FF00FF', bodyColor: 'EEEEEE', steps: [{ label: 'Later', title: 'Explore', text: 'More' }] });
  await deck.save(pres, file('timeline-colors'));
  const colorFor = (source, name) => {
    const shape = source.match(/<p:sp>.*?<\/p:sp>/g).find(block => block.includes('name="' + name + '"'));
    assert.ok(shape, name);
    return shape.match(/<a:rPr[^>]*>.*?<a:srgbClr val="([A-Fa-f0-9]+)"/)[1];
  };
  const first = await xml(file('timeline-colors'));
  assert.equal(colorFor(first, 'timeline number 1'), '000000');
  assert.equal(colorFor(first, 'timeline label 1'), 'FFFFFF');
  const second = await xml(file('timeline-colors'), 'ppt/slides/slide2.xml');
  for (const [part, color] of [['number', 'FFFF00'], ['label', '00FFFF'], ['title', 'FF00FF'], ['detail', 'EEEEEE']]) {
    assert.equal(colorFor(second, 'timeline ' + part + ' 1'), color);
  }
});
