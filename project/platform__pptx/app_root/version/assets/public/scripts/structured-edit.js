'use strict';

// Edits retain every byte outside the requested XML spans. No XML reserialization.
const fs = require('fs');
const path = require('path');
const A = 'http://schemas.openxmlformats.org/drawingml/2006/main';
const P = 'http://schemas.openxmlformats.org/presentationml/2006/main';
const fills = new Set(['noFill', 'solidFill', 'gradFill', 'blipFill', 'pattFill', 'grpFill']);

function decode(value) {
  const entities = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };
  return value.replace(/&(#x[\da-f]+|#\d+|amp|lt|gt|quot|apos);/gi, (_, v) =>
    v[0] === '#' ? String.fromCodePoint(parseInt(v.slice(v[1] === 'x' ? 2 : 1), v[1] === 'x' ? 16 : 10)) : entities[v]);
}

function encode(value) {
  if (typeof value !== 'string') throw new Error('Text must be a string');
  for (const c of value) {
    const n = c.codePointAt(0);
    if (!(n === 9 || n === 10 || n === 13 || n >= 32 && n <= 0xD7FF || n >= 0xE000 && n <= 0xFFFD || n >= 0x10000 && n <= 0x10FFFF))
      throw new Error('Text contains a character XML cannot represent');
  }
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\r/g, '&#13;');
}

function parse(xml) {
  if (/<!DOCTYPE|<!ENTITY/i.test(xml)) throw new Error('DTD declarations are unsupported');
  const stack = [], nodes = [];
  const tags = /<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|<\?[\s\S]*?\?>|<\/?[\w:.-]+(?:[^>"']|"[^"]*"|'[^']*')*>/g;
  for (const m of xml.matchAll(tags)) {
    const tag = m[0];
    if (/^<[!?]/.test(tag)) continue;
    if (tag.startsWith('</')) {
      const node = stack.pop();
      if (!node || tag.slice(2, -1).trim() !== node.name) throw new Error('Mismatched XML element');
      node.close = m.index; node.end = m.index + tag.length;
      continue;
    }
    const name = /^<([\w:.-]+)/.exec(tag)[1], parent = stack[stack.length - 1];
    const namespaces = { ...(parent ? parent.namespaces : {}) }, attrs = {};
    for (const attr of tag.matchAll(/\s([\w:.-]+)\s*=\s*(["'])(.*?)\2/g)) {
      attrs[attr[1]] = decode(attr[3]);
      if (attr[1] === 'xmlns') namespaces[''] = attrs[attr[1]];
      if (attr[1].startsWith('xmlns:')) namespaces[attr[1].slice(6)] = attrs[attr[1]];
    }
    const colon = name.indexOf(':'), prefix = colon < 0 ? '' : name.slice(0, colon);
    const empty = /\/\s*>$/.test(tag);
    const node = { name, local: name.slice(colon + 1), ns: namespaces[prefix], namespaces, attrs, parent,
      start: m.index, openEnd: m.index + tag.length, close: empty ? m.index : null,
      end: empty ? m.index + tag.length : null, empty, children: [] };
    if (parent) parent.children.push(node);
    nodes.push(node);
    if (!empty) stack.push(node);
  }
  if (stack.length || nodes.filter(n => !n.parent).length !== 1) throw new Error('Invalid slide XML');
  return nodes;
}

const child = (node, ns, local) => node && node.children.find(n => n.ns === ns && n.local === local);
const inside = (node, ancestor) => node.start > ancestor.start && node.end < ancestor.end;

function qualified(node, ns, local) {
  const prefix = Object.keys(node.namespaces).find(p => node.namespaces[p] === ns);
  if (prefix === undefined) throw new Error('Missing slide namespace');
  return prefix ? prefix + ':' + local : local;
}

function replaceInner(xml, node, content) {
  if (node.empty) return xml.slice(0, node.start) + xml.slice(node.start, node.openEnd).replace(/\/\s*>$/, '>') + content + '</' + node.name + '>' + xml.slice(node.end);
  return xml.slice(0, node.openEnd) + content + xml.slice(node.close);
}

function insert(xml, node, content, before) {
  if (node.empty) return replaceInner(xml, node, content);
  const at = before ? before.start : node.close;
  return xml.slice(0, at) + content + xml.slice(at);
}

function colorFill(node, color) {
  const fill = qualified(node, A, 'solidFill'), rgb = qualified(node, A, 'srgbClr');
  return '<' + fill + '><' + rgb + ' val="' + color + '"/></' + fill + '>';
}

function setFill(xml, node, color, before) {
  const existing = node.children.filter(n => n.ns === A && fills.has(n.local));
  if (existing.length > 1) throw new Error('Ambiguous existing fill');
  const value = colorFill(node, color);
  if (existing.length) return xml.slice(0, existing[0].start) + value + xml.slice(existing[0].end);
  return insert(xml, node, value, before);
}

function shape(nodes, id) {
  if (typeof id !== 'string' || !/^\d+$/.test(id)) throw new Error('Use the inspected objectId');
  const matches = nodes.filter(n => n.ns === P && n.local === 'sp' &&
    child(child(n, P, 'nvSpPr'), P, 'cNvPr')?.attrs.id === id);
  if (matches.length !== 1) throw new Error('Object is missing, ambiguous or not a text shape: ' + id);
  return matches[0];
}

function patch(xml, op) {
  let nodes = parse(xml);
  if (op.type === 'setBackground') {
    const cSld = child(nodes[0], P, 'cSld');
    if (!cSld) throw new Error('Missing slide content');
    const bg = child(cSld, P, 'bg'), props = child(bg, P, 'bgPr');
    if (props) return setFill(xml, props, op.color, props.children[0]);
    const bgName = qualified(cSld, P, 'bg'), propName = qualified(cSld, P, 'bgPr');
    const value = '<' + bgName + '><' + propName + '>' + colorFill(cSld, op.color) + '</' + propName + '></' + bgName + '>';
    if (bg) return xml.slice(0, bg.start) + value + xml.slice(bg.end);
    return insert(xml, cSld, value, cSld.children[0]);
  }
  const object = shape(nodes, op.objectId);
  const texts = nodes.filter(n => n.ns === A && n.local === 't');
  if (op.type === 'replaceText') {
    const node = texts[op.index];
    if (!node || !inside(node, object)) throw new Error('Text index does not belong to the inspected object');
    const raw = node.empty ? '' : xml.slice(node.openEnd, node.close);
    if (raw.includes('<') || decode(raw.replace(/\r\n?/g, '\n')) !== op.oldText)
      throw new Error('Text no longer matches the inspected object and index');
    return replaceInner(xml, node, encode(op.newText));
  }
  // Work in reverse order; offsets of earlier runs remain valid after each patch.
  const runs = nodes.filter(n => n.ns === A && ['r', 'fld'].includes(n.local) && inside(n, object) && child(n, A, 't'));
  if (!runs.length) throw new Error('Object has no editable text runs');
  for (const run of runs.reverse()) {
    const props = child(run, A, 'rPr');
    if (props) {
      // In CT_TextCharacterProperties, fill follows ln and precedes effects/fonts.
      const before = props.children.find(n => !(n.ns === A && n.local === 'ln'));
      xml = setFill(xml, props, op.color, before);
    } else {
      const name = qualified(run, A, 'rPr');
      xml = insert(xml, run, '<' + name + '>' + colorFill(run, op.color) + '</' + name + '>', run.children[0]);
    }
  }
  return xml;
}

function validateOperations(operations) {
  if (!Array.isArray(operations) || !operations.length || operations.length > 200) throw new Error('Supply 1 to 200 edit operations');
  const seen = new Set();
  for (const op of operations) {
    if (!op || !['replaceText', 'setTextColor', 'setBackground'].includes(op.type)) throw new Error('Unsupported edit operation');
    const keys = op.type === 'replaceText' ? ['type', 'part', 'objectId', 'index', 'oldText', 'newText']
      : op.type === 'setTextColor' ? ['type', 'part', 'objectId', 'color'] : ['type', 'part', 'color'];
    if (Object.keys(op).some(k => !keys.includes(k)) || keys.some(k => !Object.hasOwn(op, k))) throw new Error('Unexpected or missing operation field');
    if (typeof op.part !== 'string' || !/^ppt\/slides\/[^/]+\.xml$/.test(op.part)) throw new Error('Use the inspected slide part');
    if (op.type === 'replaceText') {
      if (!Number.isInteger(op.index) || op.index < 0 || typeof op.oldText !== 'string') throw new Error('Use the inspected text index and oldText');
      encode(op.newText);
      if (op.oldText === op.newText) throw new Error('Replacement must change text');
    } else if (typeof op.color !== 'string' || !/^[\dA-Fa-f]{6}$/.test(op.color)) throw new Error('Color must be six hexadecimal digits without #');
    const key = [op.part, op.type, op.type === 'replaceText' ? op.index : op.objectId || 'background'].join(':');
    if (seen.has(key)) throw new Error('Duplicate edit target');
    seen.add(key);
  }
}

async function apply({ JSZip, input, output, operations }) {
  validateOperations(operations);
  if (path.resolve(input) === path.resolve(output) || fs.existsSync(output) && fs.realpathSync(input) === fs.realpathSync(output))
    throw new Error('Read the protected snapshot; never overwrite it');
  const zip = await JSZip.loadAsync(fs.readFileSync(input), { checkCRC32: true });
  const changed = new Set();
  for (const op of operations) {
    const entry = zip.file(op.part);
    if (!entry) throw new Error('Missing inspected slide part: ' + op.part);
    const before = await entry.async('string'), after = patch(before, op);
    if (after !== before) { zip.file(op.part, after); changed.add(op.part); }
  }
  if (!changed.size) throw new Error('No requested edit changed the presentation');
  const bytes = await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
  fs.mkdirSync(path.dirname(output), { recursive: true });
  const temp = output + '.edit-' + require('crypto').randomUUID() + '.tmp';
  try { fs.writeFileSync(temp, bytes); fs.renameSync(temp, output); }
  finally { if (fs.existsSync(temp)) fs.unlinkSync(temp); }
  return { changedParts: [...changed], operations: operations.length };
}

module.exports = { apply };
