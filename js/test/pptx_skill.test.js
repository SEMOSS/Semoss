'use strict';

// Run from the repository root: node --test js/test/pptx_skill.test.js
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const envRequire = createRequire(path.resolve(__dirname, '../node_env/package.json'));
const JSZip = envRequire('jszip');
const skill = path.resolve(__dirname, '../../project/platform__pptx/app_root/version/assets/public');
const requestedOutput = process.env.PPTX_SKILL_TEST_OUTPUT;
const room = requestedOutput || fs.mkdtempSync(path.join(os.tmpdir(), 'semoss-pptx-skill-'));
const staged = path.join(room, '.claude/skills/pptx');
const examples = ['image-led', 'analytical', 'editorial'];
const defaultReadBytes = 8192; // LoadSkill's documented default, including the frontmatter.
const decks = new Map();

before(async () => {
  fs.mkdirSync(room, { recursive: true });
  fs.cpSync(skill, staged, { recursive: true });
  for (const name of examples) {
    const source = fs.readFileSync(path.join(staged, 'examples', name + '.js'), 'utf8');
    const validation = await vm.runInNewContext(source, { ROOT: room, require: envRequire },
      { filename: name + '.js', timeout: 30000 });
    assert.equal(validation.ok, true, JSON.stringify(validation));
    assert.equal(validation.slides, 3);
    assert.deepEqual(validation.warnings, []);
    assert.equal(validation.review.rendered, false);
    decks.set(name, await JSZip.loadAsync(fs.readFileSync(path.join(room, name + '-example.pptx'))));
  }
});

after(() => { if (!requestedOutput) fs.rmSync(room, { recursive: true, force: true }); });

test('entry and routine references fit a complete default LoadSkill read', () => {
  const files = ['SKILL.md', ...examples.map(name => 'examples/' + name + '.js'),
    'references/components.md', 'references/native-api.md', 'references/editing.md',
    'references/json-layouts.md', 'assets/CREDITS.md'];
  for (const file of files) {
    assert.ok(fs.statSync(path.join(skill, file)).size <= defaultReadBytes, file + ' would be truncated');
  }
  assert.ok(fs.statSync(path.join(skill, 'references/upstream.md')).size <= 65536,
    'The long-reference read advertised by the entry must include the whole upstream file');
});

test('routed references and the bundled image survive skill staging', () => {
  for (const reference of ['SKILL.md', 'references/editing.md']) {
    const parent = path.dirname(reference);
    const links = fs.readFileSync(path.join(skill, reference), 'utf8').matchAll(/\]\(([^)]+)\)/g);
    for (const [, target] of links) {
      if (/^https?:|^#/.test(target)) continue;
      const relative = path.join(parent, target);
      assert.ok(fs.statSync(path.join(staged, relative)).isFile(), relative);
    }
  }
  assert.deepEqual(fs.readFileSync(path.join(staged, 'assets/earthrise.jpg')),
    fs.readFileSync(path.join(skill, 'assets/earthrise.jpg')));
});

test('image example embeds its photos and preserves editable foreground text and credits', async () => {
  const zip = decks.get('image-led');
  for (let i = 1; i <= 3; i++) {
    const xml = await zip.file('ppt/slides/slide' + i + '.xml').async('string');
    assert.match(xml, /<p:pic>/);
    assert.match(xml, /<p:txBody>/);
    assert.match(xml, /NASA\/Bill Anders/);
    const notes = await zip.file('ppt/notesSlides/notesSlide' + i + '.xml').async('string');
    assert.match(notes, /science.nasa.gov/);
  }
  const pictures = zip.file(/^ppt\/media\/.*\.jpe?g$/);
  assert.ok(pictures.length >= 1);
  assert.ok((await pictures[0].async('nodebuffer')).subarray(0, 2).equals(Buffer.from([0xff, 0xd8])));
});

test('analytical example preserves editable charts, source numbers and units', async () => {
  const zip = decks.get('analytical');
  const charts = zip.file(/^ppt\/charts\/chart\d+\.xml$/);
  assert.equal(charts.length, 2);
  assert.equal(zip.file(/^ppt\/embeddings\/.*\.xlsx$/).length, 2);
  const chartText = (await Promise.all(charts.map(file => file.async('string')))).join('\n');
  for (const value of ['36', '52', '64', '48', '14.2', '9.1', '5.6', '3.8', '7.4', '4.2']) {
    assert.ok(chartText.includes('<c:v>' + value + '</c:v>'), 'Missing source value ' + value);
  }
  const slide = await zip.file('ppt/slides/slide2.xml').async('string');
  assert.match(slide, /Minutes to first response/);
  assert.match(slide, /Illustrative data only/);
});

test('editorial example uses native typography and an editable process diagram', async () => {
  const zip = decks.get('editorial');
  assert.equal(zip.file(/^ppt\/media\//).length, 0);
  const first = await zip.file('ppt/slides/slide1.xml').async('string');
  assert.match(first, /Cambria/);
  assert.match(first, /Arial/);
  const last = await zip.file('ppt/slides/slide3.xml').async('string');
  for (const label of ['Observe', 'Pilot', 'Refine']) assert.ok(last.includes(label));
  assert.equal((last.match(/prst="ellipse"/g) || []).length, 3);
  assert.match(last, /prst="line"/);
});
