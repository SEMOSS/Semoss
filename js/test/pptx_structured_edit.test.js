'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createRequire } = require('node:module');
const envRequire = createRequire(path.resolve(__dirname, '../node_env/package.json'));
const JSZip = envRequire('jszip');
const { XMLValidator } = envRequire('fast-xml-parser');
const edit = require('../../project/platform__pptx/app_root/version/assets/public/scripts/structured-edit.js');
const part = 'ppt/slides/slide7.xml';
const body = '<p:sp><p:nvSpPr><p:cNvPr id="5" name="Body"/></p:nvSpPr><p:spPr><d:xfrm><d:off x="12345" y="67890"/></d:xfrm><d:noFill/></p:spPr><p:txBody><d:bodyPr/><d:p><d:pPr><d:buChar char="•"/></d:pPr><d:r><d:rPr sz="1700" b="1"><d:solidFill><d:srgbClr val="2C2A29"/></d:solidFill><d:latin typeface="Arial"/><d:hlinkClick r:id="link1"/></d:rPr><d:t>DOGS</d:t></d:r><d:r><d:rPr i="1"/><d:t>DOGS</d:t></d:r><d:r><d:t/></d:r></d:p></p:txBody></p:sp>';
const xml = '<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" xmlns:d="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><p:cSld><p:bg><p:bgPr><d:solidFill><d:srgbClr val="F9F6F0"/></d:solidFill><d:effectLst/></p:bgPr></p:bg><p:spTree>' + body + '</p:spTree></p:cSld></p:sld>';

async function fixture(t, source = xml) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pptx-structured-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const zip = new JSZip();
  zip.file(part, source);
  for (const name of ['ppt/slides/slide2.xml', 'ppt/theme/theme1.xml', 'ppt/slideLayouts/slideLayout1.xml', 'ppt/notesSlides/notesSlide7.xml', 'ppt/slides/_rels/slide7.xml.rels', 'ppt/media/logo.png', 'ppt/embeddings/data.xlsx'])
    zip.file(name, Buffer.from([0, 255, 18, 42]));
  const input = path.join(root, 'original.pptx'), output = path.join(root, 'edited.pptx');
  fs.writeFileSync(input, await zip.generateAsync({ type: 'nodebuffer' }));
  return { zip, input, output };
}
const wording = { type: 'replaceText', part, objectId: '5', index: 0, oldText: 'DOGS', newText: 'Cats & kittens <3' };
const background = { type: 'setBackground', part, color: '000000' };
const foreground = { type: 'setTextColor', part, objectId: '5', color: 'FFFFFF' };
const content = async output => (await JSZip.loadAsync(fs.readFileSync(output), { checkCRC32: true })).file(part).async('string');

test('structured text edits patch only the exact inspected run, preserving all other bytes', async t => {
  const f = await fixture(t), original = fs.readFileSync(f.input);
  await edit.apply({ ...f, JSZip, operations: [wording] });
  assert.equal(await content(f.output), xml.replace('>DOGS<', '>Cats &amp; kittens &lt;3<'));
  const output = await JSZip.loadAsync(fs.readFileSync(f.output));
  assert.deepEqual(Object.keys(output.files).sort(), Object.keys(f.zip.files).sort());
  for (const entry of Object.values(f.zip.files).filter(e => !e.dir && e.name !== part))
    assert.deepEqual(await output.file(entry.name).async('nodebuffer'), await entry.async('nodebuffer'), entry.name);
  assert.deepEqual(fs.readFileSync(f.input), original);
});

test('background change and foreground correction save atomically without changing geometry or fonts', async t => {
  const f = await fixture(t);
  await edit.apply({ ...f, JSZip, operations: [background, foreground] });
  const actual = await content(f.output);
  const white = '<d:solidFill><d:srgbClr val="FFFFFF"/></d:solidFill>';
  const expected = xml.replace('val="F9F6F0"', 'val="000000"').replace('val="2C2A29"', 'val="FFFFFF"')
    .replace('<d:rPr i="1"/>', '<d:rPr i="1">' + white + '</d:rPr>')
    .replace('<d:r><d:t/>', '<d:r><d:rPr>' + white + '</d:rPr><d:t/>');
  assert.equal(actual, expected);
  assert.equal(XMLValidator.validate(actual), true);
});

test('inherited background gets a slide-local override without editing layouts or themes', async t => {
  const inherited = xml.replace(/<p:bg>[\s\S]*?<\/p:bg>/, '');
  const f = await fixture(t, inherited);
  await edit.apply({ ...f, JSZip, operations: [background] });
  assert.equal(await content(f.output), inherited.replace('<p:spTree>', '<p:bg><p:bgPr><d:solidFill><d:srgbClr val="000000"/></d:solidFill></p:bgPr></p:bg><p:spTree>'));
});

test('existing background gradients and theme references become a local solid background', async t => {
  for (const bg of ['<p:bg><p:bgRef idx="1001"><d:schemeClr val="bg1"/></p:bgRef></p:bg>', '<p:bg><p:bgPr><d:gradFill/><d:effectLst/></p:bgPr></p:bg>']) {
    const f = await fixture(t, xml.replace(/<p:bg>[\s\S]*?<\/p:bg>/, bg));
    await edit.apply({ ...f, JSZip, operations: [background] });
    const result = await content(f.output);
    assert.equal(XMLValidator.validate(result), true);
    assert.match(result, /<d:srgbClr val="000000"\/>/);
    assert.ok(!result.includes('gradFill') && !result.includes('bgRef'));
    assert.ok(result.includes(body));
  }
});

test('grouped text shapes use their own IDs and leave the group transform intact', async t => {
  const grouped = xml.replace(body, '<p:grpSp><p:nvGrpSpPr><p:cNvPr id="6" name="Group"/></p:nvGrpSpPr><p:grpSpPr><d:xfrm rot="60000"/></p:grpSpPr>' + body + '</p:grpSp>');
  const f = await fixture(t, grouped);
  await edit.apply({ ...f, JSZip, operations: [wording] });
  assert.equal(await content(f.output), grouped.replace('>DOGS<', '>Cats &amp; kittens &lt;3<'));
});

test('duplicate shape IDs, wrong object/index, stale text and missing parts preserve the previous output', async t => {
  const invalid = [
    { ...wording, objectId: '99' }, { ...wording, index: 99 }, { ...wording, oldText: 'CATS' },
    { ...wording, part: 'ppt/slides/missing.xml' }
  ];
  for (const op of invalid) {
    const f = await fixture(t); fs.writeFileSync(f.output, 'previous saved output');
    await assert.rejects(edit.apply({ ...f, JSZip, operations: [background, op] }));
    assert.equal(fs.readFileSync(f.output, 'utf8'), 'previous saved output');
  }
  const f = await fixture(t, xml.replace(body, body + body));
  await assert.rejects(edit.apply({ ...f, JSZip, operations: [foreground] }), /ambiguous/);
  assert.equal(fs.existsSync(f.output), false);
});

test('bad colors, unsupported operations, duplicate targets, unknown fields and invalid XML text fail before saving', async t => {
  const f = await fixture(t); fs.writeFileSync(f.output, 'saved');
  for (const operations of [[{ ...foreground, color: '#FFFFFF' }], [{ ...foreground, color: 'red' }], [{ type: 'addSlide', part }], [foreground, foreground], [{ ...background, objectId: '5' }], [{ ...wording, newText: '\u0000' }]]) {
    await assert.rejects(edit.apply({ ...f, JSZip, operations }));
    assert.equal(fs.readFileSync(f.output, 'utf8'), 'saved');
  }
});

test('repeated plans start from the original and combining text plus color works in either order', async t => {
  const f = await fixture(t);
  await edit.apply({ ...f, JSZip, operations: [wording, foreground] }); const first = await content(f.output);
  await edit.apply({ ...f, JSZip, operations: [foreground, wording] }); assert.equal(await content(f.output), first);
  await edit.apply({ ...f, JSZip, operations: [background] }); assert.equal(await content(f.output), xml.replace('val="F9F6F0"', 'val="000000"'));
  await assert.rejects(edit.apply({ ...f, JSZip, output: f.input, operations: [background] }), /snapshot/);
});

test('color fill insertion preserves schema order after outline and before effects/fonts', async t => {
  const source = xml.replace('<d:rPr i="1"/>', '<d:rPr i="1"><d:ln/><d:effectLst/><d:latin typeface="Calibri"/></d:rPr>');
  const f = await fixture(t, source);
  await edit.apply({ ...f, JSZip, operations: [foreground] });
  assert.match(await content(f.output), /<d:ln\/><d:solidFill>.*?<\/d:solidFill><d:effectLst\/><d:latin/);
});
