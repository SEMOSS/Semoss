'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createRequire } = require('node:module');
const envRequire = createRequire(path.resolve(__dirname, '../node_env/package.json'));
const JSZip = envRequire('jszip');
const edit = require('../../project/platform__pptx/app_root/version/assets/public/scripts/edit.js');
const part = 'ppt/slides/slide7.xml';
const xml = '<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" xmlns:d="http://schemas.openxmlformats.org/drawingml/2006/main"><p:sp><p:spPr><d:xfrm><d:off x="12345" y="67890"/></d:xfrm></p:spPr><p:txBody><d:p><d:r><d:rPr sz="5200" b="1"/><d:t>DOGS</d:t></d:r><d:r><d:rPr i="1"/><d:t>DOGS</d:t></d:r><d:r><d:t xml:space="preserve"> A &amp; B &#x1F431; </d:t></d:r><d:r><d:t/></d:r></d:p></p:txBody></p:sp></p:sld>';

async function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pptx-edit-test-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const zip = new JSZip();
  zip.file(part, xml);
  zip.file('ppt/slides/slide2.xml', '<original manuallyEdited="yes"/>');
  zip.file('ppt/notesSlides/notesSlide7.xml', 'Manual speaker notes');
  zip.file('ppt/media/logo.png', Buffer.from([0, 255, 128, 42]));
  zip.file('ppt/embeddings/workbook.xlsx', Buffer.from([8, 7, 6, 5]));
  zip.file('ppt/charts/chart1.xml', 'Original chart values');
  const input = path.join(root, 'original.pptx'), output = path.join(root, 'edited.pptx');
  fs.writeFileSync(input, await zip.generateAsync({ type: 'nodebuffer' }));
  return { zip, input, output };
}

test('one text edit retains exact surrounding XML, manual styles and all other parts', async t => {
  const { zip, input, output } = await fixture(t);
  await edit.replaceText({ JSZip, input, output, replacements: [{ part, index: 0, oldText: 'DOGS', newText: 'CATS' }] });
  const result = await JSZip.loadAsync(fs.readFileSync(output), { checkCRC32: true });
  assert.equal(await result.file(part).async('string'), xml.replace('>DOGS<', '>CATS<'));
  assert.deepEqual(Object.keys(result.files).sort(), Object.keys(zip.files).sort());
  for (const entry of Object.values(zip.files).filter(entry => !entry.dir && entry.name !== part))
    assert.deepEqual(await result.file(entry.name).async('nodebuffer'), await entry.async('nodebuffer'), entry.name);
  assert.equal(await (await JSZip.loadAsync(fs.readFileSync(input))).file(part).async('string'), xml);
});

test('node index disambiguates repeated text and retains mixed run formatting', async t => {
  const { input, output } = await fixture(t);
  await edit.replaceText({ JSZip, input, output, replacements: [{ part, index: 1, oldText: 'DOGS', newText: 'cats & kittens <3' }] });
  const result = await JSZip.loadAsync(fs.readFileSync(output));
  assert.equal(await result.file(part).async('string'), xml.replace('<d:rPr i="1"/><d:t>DOGS</d:t>', '<d:rPr i="1"/><d:t>cats &amp; kittens &lt;3</d:t>'));
});

test('entities, alternate namespace prefix and empty nodes keep the inspection indexes', async t => {
  const { input, output } = await fixture(t);
  await edit.replaceText({ JSZip, input, output, replacements: [
    { part, index: 2, oldText: ' A & B 🐱 ', newText: ' Cats & Dogs ' },
    { part, index: 3, oldText: '', newText: 'New text' }
  ] });
  const result = await JSZip.loadAsync(fs.readFileSync(output));
  assert.equal(await result.file(part).async('string'), xml.replace(' A &amp; B &#x1F431; ', ' Cats &amp; Dogs ').replace('<d:t/>', '<d:t>New text</d:t>'));
});

test('stale matches and duplicate indexes fail atomically without touching output', async t => {
  const { input, output } = await fixture(t);
  fs.writeFileSync(output, 'previous output');
  for (const second of [{ part, index: 1, oldText: 'wrong', newText: 'cats' }, { part, index: 0, oldText: 'DOGS', newText: 'kittens' }]) {
    await assert.rejects(edit.replaceText({ JSZip, input, output, replacements: [{ part, index: 0, oldText: 'DOGS', newText: 'CATS' }, second] }), /uniquely match/);
    assert.equal(fs.readFileSync(output, 'utf8'), 'previous output');
  }
});

test('repeated builds read the preserved source and invalid XML characters do not overwrite output', async t => {
  const { input, output } = await fixture(t);
  for (const newText of ['CATS', 'KITTENS']) await edit.replaceText({ JSZip, input, output, replacements: [{ part, index: 0, oldText: 'DOGS', newText }] });
  const bytes = fs.readFileSync(output);
  assert.match(await (await JSZip.loadAsync(bytes)).file(part).async('string'), />KITTENS</);
  await assert.rejects(edit.replaceText({ JSZip, input, output, replacements: [{ part, index: 0, oldText: 'DOGS', newText: '\u0000' }] }), /XML/);
  assert.deepEqual(fs.readFileSync(output), bytes);
  await assert.rejects(edit.replaceText({ JSZip, input, output: input, replacements: [{ part, index: 0, oldText: 'DOGS', newText: 'CATS' }] }), /snapshot/);
});
