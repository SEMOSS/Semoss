'use strict';

// Patch text spans without reserializing the surrounding DrawingML.
const fs = require('fs');
const path = require('path');
const drawing = 'http://schemas.openxmlformats.org/drawingml/2006/main';

function decode(text) {
  const entities = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };
  return text.replace(/&(#x[0-9a-fA-F]+|#\d+|amp|lt|gt|quot|apos);/g, (_, value) =>
    value[0] === '#' ? String.fromCodePoint(parseInt(value.slice(value[1] === 'x' ? 2 : 1), value[1] === 'x' ? 16 : 10)) : entities[value]);
}

function encode(text) {
  for (const character of text) {
    const code = character.codePointAt(0);
    if (!(code === 9 || code === 10 || code === 13 || code >= 32 && code <= 0xD7FF || code >= 0xE000 && code <= 0xFFFD || code >= 0x10000 && code <= 0x10FFFF))
      throw new Error('Replacement contains a character XML cannot represent');
  }
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\r/g, '&#13;');
}

function textNodes(xml) {
  const nodes = [], stack = [];
  const tags = /<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|<\?[\s\S]*?\?>|<\/?[\w:.-]+(?:[^>"']|"[^"]*"|'[^']*')*>/g;
  for (const match of xml.matchAll(tags)) {
    const tag = match[0];
    if (/^<[!?]/.test(tag)) continue;
    if (tag.startsWith('</')) {
      const item = stack.pop();
      if (item && item.text) {
        const raw = xml.slice(item.start, match.index);
        if (raw.includes('<')) throw new Error('Complex text content needs a scoped XML edit; do not rebuild the deck');
        nodes.push({ start: item.start, end: match.index, text: decode(raw.replace(/\r\n?/g, '\n')) });
      }
      continue;
    }
    const name = /^<([\w:.-]+)/.exec(tag)[1];
    const namespaces = Object.assign({}, stack.length ? stack[stack.length - 1].namespaces : {});
    for (const attr of tag.matchAll(/\sxmlns(?::([\w.-]+))?\s*=\s*(["'])(.*?)\2/g)) namespaces[attr[1] || ''] = decode(attr[3]);
    const colon = name.indexOf(':'), prefix = colon < 0 ? '' : name.slice(0, colon), local = name.slice(colon + 1);
    const item = { namespaces, text: local === 't' && namespaces[prefix] === drawing, start: match.index + tag.length };
    if (tag.endsWith('/>')) {
      if (item.text) nodes.push({ start: match.index, end: match.index + tag.length, text: '', emptyTag: tag, name });
    } else stack.push(item);
  }
  return nodes;
}

async function replaceText({ JSZip, input, output, replacements }) {
  if (!JSZip || !Array.isArray(replacements) || !replacements.length) throw new Error('Supply JSZip, input, output and replacements');
  if (path.resolve(input) === path.resolve(output)) throw new Error('Read the protected inputSnapshot and save to the requested output; never overwrite the snapshot');
  const zip = await JSZip.loadAsync(fs.readFileSync(input), { checkCRC32: true });
  const byPart = new Map();
  for (const change of replacements) {
    if (typeof change.part !== 'string' || !Number.isInteger(change.index) || change.index < 0 ||
        typeof change.oldText !== 'string' || typeof change.newText !== 'string') throw new Error('Each replacement needs the inspected part, index, oldText and newText');
    if (change.oldText === change.newText) throw new Error('Replacement must change the requested text');
    if (!byPart.has(change.part)) byPart.set(change.part, []);
    byPart.get(change.part).push(change);
  }
  for (const [part, changes] of byPart) {
    const entry = zip.file(part);
    if (!entry) throw new Error('Missing inspected slide part: ' + part);
    let xml = await entry.async('string');
    const nodes = textNodes(xml), seen = new Set(), patches = [];
    for (const change of changes) {
      const node = nodes[change.index];
      if (!node || node.text !== change.oldText || seen.has(change.index))
        throw new Error('Text does not uniquely match the inspected node: ' + part + ' index ' + change.index);
      seen.add(change.index);
      const escaped = encode(change.newText);
      patches.push({ ...node, value: node.emptyTag ? node.emptyTag.slice(0, -2) + '>' + escaped + '</' + node.name + '>' : escaped });
    }
    for (const patch of patches.sort((a, b) => b.start - a.start)) xml = xml.slice(0, patch.start) + patch.value + xml.slice(patch.end);
    zip.file(part, xml);
  }
  const bytes = await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
  fs.mkdirSync(path.dirname(output), { recursive: true });
  const temp = output + '.edit-' + require('crypto').randomUUID() + '.tmp';
  try { fs.writeFileSync(temp, bytes); fs.renameSync(temp, output); }
  finally { if (fs.existsSync(temp)) fs.unlinkSync(temp); }
  return { changedParts: [...byPart.keys()], replacements: replacements.length };
}

module.exports = { replaceText };
