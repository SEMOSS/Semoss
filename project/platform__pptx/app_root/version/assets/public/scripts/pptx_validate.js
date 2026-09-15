'use strict';

// Package checks for deck.js. Node core only: skill copies live outside node_env.
// This checks file structure and geometry, not rendered text fit or appearance.
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const A = 'http://schemas.openxmlformats.org/drawingml/2006/main';
const P = 'http://schemas.openxmlformats.org/presentationml/2006/main';
const C = 'http://schemas.openxmlformats.org/drawingml/2006/chart';
const R = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships';
const REL = 'http://schemas.openxmlformats.org/package/2006/relationships';
const DC = 'http://purl.org/dc/elements/1.1/';
const EMU = 914400;

const CRC_TABLE = Array.from({ length: 256 }, function (_, n) {
	for (let bit = 0; bit < 8; bit++) { n = (n & 1) ? (0xedb88320 ^ (n >>> 1)) : (n >>> 1); }
	return n >>> 0;
});

function crc32(data) {
	let crc = 0xffffffff;
	for (const byte of data) { crc = CRC_TABLE[(crc ^ byte) & 255] ^ (crc >>> 8); }
	return (crc ^ 0xffffffff) >>> 0;
}

function readZip(buffer) {
	function need(ok, message) { if (!ok) { throw new Error(message); } }
	function range(offset, length, limit) {
		need(offset >= 0 && length >= 0 && offset + length <= (limit === undefined ? buffer.length : limit),
			'truncated ZIP record or entry');
	}
	need(buffer.length >= 22 && buffer.readUInt32LE(0) === 0x04034b50, 'not a valid .pptx ZIP archive');
	let end = -1;
	for (let i = buffer.length - 22; i >= Math.max(0, buffer.length - 65557); i--) {
		if (buffer.readUInt32LE(i) === 0x06054b50 && i + 22 + buffer.readUInt16LE(i + 20) === buffer.length) {
			end = i;
			break;
		}
	}
	need(end >= 0, 'missing ZIP end-of-central-directory record');
	const count = buffer.readUInt16LE(end + 10);
	need(buffer.readUInt16LE(end + 4) === 0 && buffer.readUInt16LE(end + 6) === 0
		&& buffer.readUInt16LE(end + 8) === count, 'multi-disk ZIP is unsupported');
	const centralSize = buffer.readUInt32LE(end + 12);
	const centralStart = buffer.readUInt32LE(end + 16);
	need(count !== 0xffff && centralStart !== 0xffffffff && centralSize !== 0xffffffff, 'ZIP64 is unsupported');
	range(centralStart, centralSize, end);
	let offset = centralStart;
	const entries = new Map();
	for (let i = 0; i < count; i++) {
		range(offset, 46, centralStart + centralSize);
		need(buffer.readUInt32LE(offset) === 0x02014b50, 'bad central-directory signature');
		const flags = buffer.readUInt16LE(offset + 8);
		const method = buffer.readUInt16LE(offset + 10);
		const crc = buffer.readUInt32LE(offset + 16);
		const compressed = buffer.readUInt32LE(offset + 20);
		const uncompressed = buffer.readUInt32LE(offset + 24);
		const nameLength = buffer.readUInt16LE(offset + 28);
		const extraLength = buffer.readUInt16LE(offset + 30);
		const commentLength = buffer.readUInt16LE(offset + 32);
		const local = buffer.readUInt32LE(offset + 42);
		range(offset + 46, nameLength + extraLength + commentLength, centralStart + centralSize);
		const nameBytes = buffer.subarray(offset + 46, offset + 46 + nameLength);
		const name = nameBytes.toString('utf8');
		need(!entries.has(name), 'duplicate ZIP entry ' + name);
		need(!(flags & 1), 'encrypted ZIP entry ' + name);
		need(method === 0 || method === 8, 'unsupported ZIP compression for ' + name);
		need(buffer.readUInt16LE(offset + 34) === 0, 'multi-disk ZIP entry ' + name);
		range(local, 30, centralStart);
		need(buffer.readUInt32LE(local) === 0x04034b50, 'bad local header for ' + name);
		need(buffer.readUInt16LE(local + 6) === flags && buffer.readUInt16LE(local + 8) === method,
			'local/central header mismatch for ' + name);
		const localNameLength = buffer.readUInt16LE(local + 26);
		const localExtraLength = buffer.readUInt16LE(local + 28);
		range(local + 30, localNameLength + localExtraLength, centralStart);
		need(nameBytes.equals(buffer.subarray(local + 30, local + 30 + localNameLength)), 'ZIP filename mismatch');
		const dataStart = local + 30 + localNameLength + localExtraLength;
		range(dataStart, compressed, centralStart);
		if (!(flags & 8)) {
			need(buffer.readUInt32LE(local + 14) === crc && buffer.readUInt32LE(local + 18) === compressed
				&& buffer.readUInt32LE(local + 22) === uncompressed, 'ZIP size/CRC header mismatch for ' + name);
		} else {
			let descriptor = dataStart + compressed;
			range(descriptor, 12, centralStart);
			if (buffer.readUInt32LE(descriptor) === 0x08074b50) { descriptor += 4; }
			range(descriptor, 12, centralStart);
			need(buffer.readUInt32LE(descriptor) === crc && buffer.readUInt32LE(descriptor + 4) === compressed
				&& buffer.readUInt32LE(descriptor + 8) === uncompressed, 'ZIP data descriptor mismatch for ' + name);
		}
		const raw = buffer.subarray(dataStart, dataStart + compressed);
		const data = method === 0 ? raw : zlib.inflateRawSync(raw, { maxOutputLength: uncompressed + 1 });
		need(data.length === uncompressed, 'uncompressed size mismatch for ' + name);
		need(crc32(data) === crc, 'CRC32 mismatch for ' + name);
		entries.set(name, data);
		offset += 46 + nameLength + extraLength + commentLength;
	}
	need(offset === centralStart + centralSize, 'central-directory size/count mismatch');
	return entries;
}

function decodeXml(value) {
	if (/[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(value)) { throw new Error('invalid XML control character'); }
	if (/&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[\da-fA-F]+);)/.test(value)) { throw new Error('invalid XML entity'); }
	const named = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };
	return value.replace(/&([^;]+);/g, function (_, entity) {
		if (named[entity]) { return named[entity]; }
		const point = entity[1] === 'x' ? parseInt(entity.slice(2), 16) : Number(entity.slice(1));
		if (!(point === 9 || point === 10 || point === 13 || (point >= 32 && point <= 0xd7ff)
			|| (point >= 0xe000 && point <= 0xfffd) || (point >= 0x10000 && point <= 0x10ffff))) {
			throw new Error('invalid XML character reference');
		}
		return String.fromCodePoint(point);
	});
}

// Small namespace-aware, non-validating XML reader. It checks well-formedness
// and never loads DTDs/entities or rewrites the input package.
function parseXml(xml) {
	const document = { children: [], text: '', namespaces: { xml: 'http://www.w3.org/XML/1998/namespace' } };
	const stack = [document];
	const tokens = /<\?[\s\S]*?\?>|<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|<(?:[^<>"']|"[^"]*"|'[^']*')*>/g;
	let end = 0;
	let match;
	function append(value, cdata) {
		if (!cdata && value.includes('<')) { throw new Error('malformed XML tag'); }
		const decoded = cdata ? value : decodeXml(value);
		if (stack.length === 1 && decoded.trim()) { throw new Error('text outside XML root'); }
		stack[stack.length - 1].text += decoded;
	}
	while ((match = tokens.exec(xml)) !== null) {
		append(xml.slice(end, match.index), false);
		end = tokens.lastIndex;
		const token = match[0];
		if (token.startsWith('<?') || token.startsWith('<!--')) { continue; }
		if (token.startsWith('<![CDATA[')) { append(token.slice(9, -3), true); continue; }
		if (token.startsWith('<!')) { throw new Error('DTDs/declarations are unsupported in OOXML parts'); }
		if (token.startsWith('</')) {
			const close = /^<\/([\w.:-]+)\s*>$/.exec(token);
			if (!close || stack.length === 1 || stack.pop().name !== close[1]) { throw new Error('mismatched XML closing tag'); }
			continue;
		}
		const open = /^<([A-Za-z_][\w.:-]*)([\s\S]*?)(\/?)>$/.exec(token);
		if (!open) { throw new Error('malformed XML start tag'); }
		const parent = stack[stack.length - 1];
		const namespaces = Object.assign(Object.create(null), parent.namespaces);
		const attrs = Object.create(null);
		const attr = /\s+([A-Za-z_][\w.:-]*)\s*=\s*(?:"([^"]*)"|'([^']*)')/gy;
		let at = 0;
		let item;
		while ((item = attr.exec(open[2])) !== null) {
			at = attr.lastIndex;
			if (Object.hasOwn(attrs, item[1])) { throw new Error('duplicate XML attribute'); }
			const raw = item[2] === undefined ? item[3] : item[2];
			if (raw.includes('<')) { throw new Error('unescaped < in XML attribute'); }
			attrs[item[1]] = decodeXml(raw);
			if (item[1] === 'xmlns') { namespaces[''] = attrs[item[1]]; }
			if (item[1].startsWith('xmlns:')) { namespaces[item[1].slice(6)] = attrs[item[1]]; }
		}
		if (open[2].slice(at).trim()) { throw new Error('malformed XML attribute'); }
		const parts = open[1].split(':');
		if (parts.length > 2 || (parts.length === 2 && !namespaces[parts[0]])) { throw new Error('undeclared XML namespace'); }
		for (const key of Object.keys(attrs)) {
			if (key.includes(':') && !key.startsWith('xmlns:') && !namespaces[key.split(':')[0]]) {
				throw new Error('undeclared XML attribute namespace');
			}
		}
		const node = { name: open[1], local: parts[parts.length - 1],
			ns: namespaces[parts.length === 2 ? parts[0] : ''] || '', attrs: attrs,
			namespaces: namespaces, children: [], text: '' };
		parent.children.push(node);
		if (!open[3]) { stack.push(node); }
		if (stack.length > 256) { throw new Error('XML nesting is too deep'); }
	}
	append(xml.slice(end), false);
	if (stack.length !== 1 || document.children.length !== 1) { throw new Error('incomplete XML document'); }
	return document.children[0];
}

function is(node, ns, local) { return node && node.ns === ns && node.local === local; }
function child(node, ns, local) { return node && node.children.find(function (n) { return is(n, ns, local); }); }
function walk(node, fn) { if (node) { fn(node); node.children.forEach(function (n) { walk(n, fn); }); } }
function all(node, ns, local) {
	const matches = [];
	walk(node, function (n) { if (is(n, ns, local)) { matches.push(n); } });
	return matches;
}
function relId(node) {
	const key = Object.keys(node.attrs).find(function (k) {
		const parts = k.split(':');
		return parts.length === 2 && parts[1] === 'id' && node.namespaces[parts[0]] === R;
	});
	return key && node.attrs[key];
}

// Affine transforms keep group coordinates, rotation, and flip flags honest.
const IDENTITY = [1, 0, 0, 1, 0, 0];
function multiply(a, b) {
	return [a[0] * b[0] + a[2] * b[1], a[1] * b[0] + a[3] * b[1],
		a[0] * b[2] + a[2] * b[3], a[1] * b[2] + a[3] * b[3],
		a[0] * b[4] + a[2] * b[5] + a[4], a[1] * b[4] + a[3] * b[5] + a[5]];
}
function transform(xfrm, group) {
	const off = child(xfrm, A, 'off');
	const ext = child(xfrm, A, 'ext');
	if (!off || !ext) { return null; }
	const x = Number(off.attrs.x), y = Number(off.attrs.y), w = Number(ext.attrs.cx), h = Number(ext.attrs.cy);
	if (![x, y, w, h].every(Number.isFinite) || w < 0 || h < 0) { throw new Error('invalid shape transform'); }
	const rotation = Number(xfrm.attrs.rot || 0) * Math.PI / 10800000;
	const flipX = ['1', 'true'].includes(xfrm.attrs.flipH) ? -1 : 1;
	const flipY = ['1', 'true'].includes(xfrm.attrs.flipV) ? -1 : 1;
	const cos = Math.cos(rotation), sin = Math.sin(rotation);
	let matrix = multiply([1, 0, 0, 1, x + w / 2, y + h / 2],
		multiply([cos * flipX, sin * flipX, -sin * flipY, cos * flipY, 0, 0], [1, 0, 0, 1, -w / 2, -h / 2]));
	if (group) {
		const chOff = child(xfrm, A, 'chOff');
		const chExt = child(xfrm, A, 'chExt');
		if (!chOff || !chExt || Number(chExt.attrs.cx) === 0 || Number(chExt.attrs.cy) === 0) { return { matrix: IDENTITY, w: w, h: h }; }
		const sx = w / Number(chExt.attrs.cx), sy = h / Number(chExt.attrs.cy);
		matrix = multiply(matrix, [sx, 0, 0, sy, -Number(chOff.attrs.x) * sx, -Number(chOff.attrs.y) * sy]);
	}
	if (!matrix.every(Number.isFinite)) { throw new Error('invalid rotation or group transform'); }
	return { matrix: matrix, w: w, h: h };
}

function checkCanvas(slide, width, height, issues, label) {
	function visit(node, parent) {
		let matrix = parent;
		if (is(node, P, 'grpSp') || is(node, P, 'spTree')) {
			const group = transform(child(child(node, P, 'grpSpPr'), A, 'xfrm'), true);
			if (group) { matrix = multiply(parent, group.matrix); }
		} else if (node.ns === P && ['sp', 'pic', 'cxnSp', 'graphicFrame'].includes(node.local)) {
			const xfrm = child(child(node, P, 'spPr'), A, 'xfrm') || child(node, P, 'xfrm');
			const box = transform(xfrm, false);
			if (box) {
				const m = multiply(parent, box.matrix);
				const corners = [[0, 0], [box.w, 0], [0, box.h], [box.w, box.h]].map(function (p) {
					return [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
				});
				const tolerance = 0.01 * EMU;
				if (corners.some(function (p) { return p[0] < -tolerance || p[1] < -tolerance || p[0] > width + tolerance || p[1] > height + tolerance; })) {
					const name = all(node, P, 'cNvPr')[0];
					issues.push(label + ': off-canvas ' + node.local + (name ? ' "' + name.attrs.name + '"' : '')
						+ '; keep geometry within ' + (width / EMU).toFixed(3) + ' x ' + (height / EMU).toFixed(3) + ' inches.');
				}
			}
		}
		node.children.forEach(function (n) { visit(n, matrix); });
	}
	visit(slide, IDENTITY);
}

function validate(file, options) {
	const opts = options || {};
	const result = { ok: false, file: String(file), slides: 0, expectedSlides: null, checks: [], errors: [], warnings: [] };
	function check(id, run) {
		const issues = [];
		try { run(issues); } catch (err) { issues.push(err.message); }
		result.checks.push({ id: id, ok: issues.length === 0, errors: issues });
		issues.forEach(function (issue) { result.errors.push(id + ': ' + issue); });
		return issues.length === 0;
	}
	let entries;
	if (!check('zip', function () {
		const buffer = fs.readFileSync(file);
		result.bytes = buffer.length;
		entries = readZip(buffer);
	})) { return result; }
	check('required-parts', function (issues) {
		['[Content_Types].xml', '_rels/.rels', 'ppt/presentation.xml', 'ppt/_rels/presentation.xml.rels'].forEach(function (name) {
			if (!entries.has(name)) { issues.push('missing ' + name); }
		});
	});
	const docs = new Map();
	check('xml', function (issues) {
		entries.forEach(function (data, name) {
			if (!/\.(?:xml|rels)$/.test(name)) { return; }
			try { docs.set(name, parseXml(data.toString('utf8'))); } catch (err) { issues.push(name + ': ' + err.message); }
		});
	});
	const presentation = docs.get('ppt/presentation.xml');
	const size = child(presentation, P, 'sldSz');
	const width = size && Number(size.attrs.cx), height = size && Number(size.attrs.cy);
	const slideNames = Array.from(entries.keys()).filter(function (name) { return /^ppt\/slides\/[^/]+\.xml$/.test(name); });
	result.slides = slideNames.length;
	const subject = all(docs.get('docProps/core.xml'), DC, 'subject')[0];
	const marker = subject && /^SEMOSS JSON deck spec: (\d+) slides$/.exec(subject.text);
	const expected = opts.slides === undefined ? (marker ? Number(marker[1]) : undefined)
		: (Array.isArray(opts.slides) ? opts.slides.length : Number(opts.slides));
	result.expectedSlides = expected === undefined ? null : expected;
	check('slide-count', function (issues) {
		if (!is(presentation, P, 'presentation')) { issues.push('missing presentation root'); return; }
		if (!slideNames.length) { issues.push('no slides'); }
		if (expected !== undefined && (!Number.isInteger(expected) || expected < 1 || expected !== slideNames.length)) {
			issues.push('expected ' + expected + ' slides but found ' + slideNames.length + '; preserve the requested count.');
		}
		if (marker && Number(marker[1]) !== slideNames.length) { issues.push('slide count differs from the embedded deck spec count'); }
		const ids = all(child(presentation, P, 'sldIdLst'), P, 'sldId');
		const relationships = new Map(all(docs.get('ppt/_rels/presentation.xml.rels'), REL, 'Relationship').map(function (n) { return [n.attrs.Id, n.attrs]; }));
		const targets = new Set();
		const numbers = new Set();
		ids.forEach(function (id) {
			const relationship = relationships.get(relId(id));
			if (!relationship || relationship.TargetMode === 'External' || relationship.Type !== R + '/slide') {
				issues.push('slide id ' + id.attrs.id + ' has no internal slide relationship'); return;
			}
			const target = relationship.Target.startsWith('/') ? relationship.Target.slice(1) : path.posix.normalize(path.posix.join('ppt', relationship.Target));
			if (!entries.has(target)) { issues.push('missing referenced slide ' + target); }
			if (targets.has(target) || numbers.has(id.attrs.id)) { issues.push('duplicate slide id or target'); }
			targets.add(target);
			numbers.add(id.attrs.id);
		});
		if (ids.length !== slideNames.length || targets.size !== slideNames.length || slideNames.some(function (n) { return !targets.has(n); })) {
			issues.push('slide parts and presentation slide list disagree');
		}
	});
	check('presentation-order', function (issues) {
		if (!presentation) { issues.push('missing presentation.xml'); return; }
		const suffix = ['sldSz', 'notesSz', 'smartTags', 'embeddedFontLst', 'custShowLst', 'photoAlbum', 'custDataLst', 'kinsoku', 'defaultTextStyle', 'modifyVerifier', 'extLst'];
		const pptxgenOrder = ['sldMasterIdLst', 'sldIdLst', 'notesMasterIdLst', 'handoutMasterIdLst'].concat(suffix);
		const schemaOrder = ['sldMasterIdLst', 'notesMasterIdLst', 'handoutMasterIdLst', 'sldIdLst'].concat(suffix);
		const names = presentation.children.filter(function (n) { return n.ns === P; }).map(function (n) { return n.local; });
		function follows(order) {
			let previous = -1;
			return names.every(function (name) { const at = order.indexOf(name); const valid = at > previous; previous = at; return valid; });
		}
		// The library deliberately puts notes masters AFTER the slide list for
		// PowerPoint compatibility. Native Office files may use schema order.
		if (!follows(pptxgenOrder) && ((marker || opts.pptxgenjs) || !follows(schemaOrder))) {
			issues.push('unexpected <p:presentation> child order; preserve the generator order (pptxgenjs puts notesMasterIdLst after sldIdLst).');
		}
	});
	check('extents', function (issues) {
		docs.forEach(function (doc, name) {
			walk(doc, function (n) {
				if (n.ns !== A || !['ext', 'chExt'].includes(n.local) || n.attrs.cx === undefined) { return; }
				if (!/^\d+$/.test(n.attrs.cx) || !/^\d+$/.test(n.attrs.cy)) {
					issues.push(name + ': negative or invalid extent (' + n.attrs.cx + ', ' + n.attrs.cy + '); use w/h >= 0 and flipH/flipV.');
				}
			});
		});
	});
	check('canvas', function (issues) {
		if (!(width > 0 && height > 0)) { issues.push('invalid or missing slide dimensions'); return; }
		slideNames.forEach(function (name) {
			try {
				if (!docs.has(name)) { issues.push(name + ': missing parsed slide'); return; }
				checkCanvas(docs.get(name), width, height, issues, name);
			} catch (err) { issues.push(name + ': ' + err.message); }
		});
	});
	check('colors', function (issues) {
		docs.forEach(function (doc, name) {
			all(doc, A, 'srgbClr').forEach(function (n) {
				if (!/^[\da-fA-F]{6}$/.test(n.attrs.val || '')) { issues.push(name + ': invalid color "' + n.attrs.val + '"; use 6 hex digits, no # or alpha.'); }
			});
		});
	});
	const chartDocs = Array.from(docs.entries()).filter(function (entry) { return /^ppt\/charts\/[^/]+\.xml$/.test(entry[0]); });
	check('chart-axes', function (issues) {
		chartDocs.forEach(function (entry) {
			const name = entry[0], doc = entry[1], declared = new Map();
			walk(doc, function (n) {
				if (n.ns !== C || !['valAx', 'catAx', 'dateAx', 'serAx'].includes(n.local)) { return; }
				const id = child(n, C, 'axId');
				if (!id || !/^\d+$/.test(id.attrs.val || '') || declared.has(id.attrs.val)) { issues.push(name + ': missing, invalid or duplicate axis declaration'); return; }
				declared.set(id.attrs.val, n);
			});
			declared.forEach(function (axis) {
				const cross = child(axis, C, 'crossAx');
				if (cross && !declared.has(cross.attrs.val)) { issues.push(name + ': crossAx references undeclared axis ' + cross.attrs.val); }
			});
			walk(doc, function (n) {
				if (n.ns !== C || !/Chart$/.test(n.local)) { return; }
				if (['pieChart', 'pie3DChart', 'doughnutChart', 'ofPieChart'].includes(n.local)) { return; }
				const refs = n.children.filter(function (c) { return is(c, C, 'axId'); }).map(function (c) { return c.attrs.val; });
				if (refs.length < 2 || new Set(refs.slice(0, 2)).size < 2 || refs.slice(0, 2).some(function (id) { return !declared.has(id); })) {
					issues.push(name + ': ' + n.local + ' needs two distinct declared axes; secondary axes need both valAxes and catAxes with two entries each.');
				}
				refs.forEach(function (id, i) {
					if (declared.has(id)) { return; }
					// PptxGenJS 4 emits this unused series-axis marker on its 2D
					// bar/line/area/radar charts. PowerPoint accepts it when X/Y resolve.
					if (i === 2 && refs.length === 3 && id === '2094734556' && ['barChart', 'lineChart', 'areaChart', 'radarChart'].includes(n.local)) { return; }
					issues.push(name + ': ' + n.local + ' references undeclared axis ' + id);
				});
			});
		});
	});
	check('stacked-labels', function (issues) {
		chartDocs.forEach(function (entry) {
			walk(entry[1], function (n) {
				if (n.ns !== C || !['barChart', 'bar3DChart'].includes(n.local)) { return; }
				const grouping = child(n, C, 'grouping');
				if (!grouping || !['stacked', 'percentStacked'].includes(grouping.attrs.val)) { return; }
				all(n, C, 'dLblPos').forEach(function (position) {
					if (!['ctr', 'inEnd', 'inBase'].includes(position.attrs.val)) {
						issues.push(entry[0] + ': stacked bar labels must use ctr, inEnd or inBase, not ' + position.attrs.val + '.');
					}
				});
			});
		});
	});
	result.ok = result.errors.length === 0;
	return result;
}

module.exports = validate;
