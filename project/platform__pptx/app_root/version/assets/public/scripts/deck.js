'use strict';
/*
 * Native PowerPoint authoring with optional components and a legacy JSON renderer.
 * create() returns a native PptxGenJS presentation: combine components with any
 * slide.add* calls, then save() and validate(). See SKILL.md for a complete example.
 *
 * Inject PptxGenJS from the curated environment. This skill can be staged outside
 * node_env and only requires Node core modules itself. Execute agent scripts in
 * one async IIFE, await all work, and write to an absolute path under ROOT.
 */

const fs = require('fs');
const path = require('path');
const validatePackage = require('./pptx_validate.js');

/* ------------------------------ geometry -------------------------------- */
/* LAYOUT_WIDE = 13.333in x 7.5in. Everything below stays inside these bounds
 * (pptxgenjs writes out-of-bounds coordinates verbatim: the shape simply is
 * not on the slide). */
const SLIDE_W = 13.333;
const SLIDE_H = 7.5;
const MARGIN = 0.62;
const CONTENT_W = SLIDE_W - MARGIN * 2;
const TITLE_Y = 0.52;
const TITLE_H = 0.95;
const BODY_Y = 1.78;
const BODY_H = 4.9;
const FOOTER_Y = 6.92;

const KINDS = ['title', 'section', 'bullets', 'two-col', 'stat-row', 'quote', 'table', 'chart', 'image'];

/* ------------------------------- themes --------------------------------- */
/* 6-hex only, no leading "#", no alpha: both corrupt the file. */
const THEMES = {
	navy: {
		bg: 'FFFFFF', panel: 'F2F5FA', ink: '10243D', muted: '4A5C72',
		accent: '1F5FA8', accentSoft: 'D6E4F5', invertInk: 'FFFFFF', bandBg: '10243D',
		series: ['1F5FA8', '4F9CD9', '9CC6EA', 'C9A227', '6B7C93']
	},
	slate: {
		bg: 'FFFFFF', panel: 'F4F4F6', ink: '1D2025', muted: '5A606B',
		accent: '3D6B7D', accentSoft: 'DCE8EC', invertInk: 'FFFFFF', bandBg: '1D2025',
		series: ['3D6B7D', '6FA0AE', 'A9C6CE', 'C77B4E', '7A818C']
	},
	forest: {
		bg: 'FFFFFF', panel: 'F1F6F1', ink: '14301F', muted: '47604F',
		accent: '2E6B43', accentSoft: 'D8E9DD', invertInk: 'FFFFFF', bandBg: '14301F',
		series: ['2E6B43', '5D9B72', '9AC4A7', 'C9A227', '6B7C93']
	},
	plum: {
		bg: 'FFFFFF', panel: 'F6F2F7', ink: '2B1733', muted: '5F4A68',
		accent: '6B3F86', accentSoft: 'E4D6EC', invertInk: 'FFFFFF', bandBg: '2B1733',
		series: ['6B3F86', '9B6FB0', 'C4A6D2', 'C9A227', '6B7C93']
	},
	mono: {
		bg: 'FFFFFF', panel: 'F3F3F3', ink: '1A1A1A', muted: '595959',
		accent: '333333', accentSoft: 'DDDDDD', invertInk: 'FFFFFF', bandBg: '1A1A1A',
		series: ['333333', '666666', '999999', 'BBBBBB', '888888']
	}
};

const DEFAULT_FONTS = { heading: 'Arial', body: 'Arial' };
const renderedSlideCounts = new Map();
const presentations = new WeakMap();
const slideContexts = new WeakMap();

/* ------------------------------ utilities ------------------------------- */

function isPlainObject(value) {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function text(value) {
	if (value === null || value === undefined) {
		return '';
	}
	return String(value);
}

/* accepts "#1F5FA8" / "1f5fa8" and rejects alpha ("00000020"), which corrupts
 * the package rather than failing loudly in PowerPoint */
function hex(value, fallback) {
	const raw = text(value).trim().replace(/^#/, '');
	if (/^[0-9a-fA-F]{6}$/.test(raw)) {
		return raw.toUpperCase();
	}
	if (raw !== '') {
		throw new Error('Invalid color "' + value + '": use 6 hex digits with no "#" and no alpha (e.g. "1F5FA8")');
	}
	return fallback;
}

function resolveTheme(spec) {
	const requested = spec && spec.theme;
	const preset = typeof requested === 'string' ? requested : requested && requested.preset;
	const base = THEMES[text(preset).trim().toLowerCase()] || THEMES.navy;
	const merged = Object.assign({}, base, { series: base.series.slice(), fonts: Object.assign({}, DEFAULT_FONTS) });
	if (isPlainObject(requested)) {
		const palette = isPlainObject(requested.palette) ? requested.palette : requested;
		Object.keys(base).forEach(function (key) {
			if (key === 'series') {
				if (Array.isArray(palette.series) && palette.series.length > 0) {
					merged.series = palette.series.map(function (c) { return hex(c, base.accent); });
				}
				return;
			}
			merged[key] = hex(palette[key], merged[key]);
		});
		if (isPlainObject(requested.fonts)) {
			Object.keys(DEFAULT_FONTS).forEach(function (key) {
				merged.fonts[key] = text(requested.fonts[key]).trim() || DEFAULT_FONTS[key];
			});
		}
	}
	return merged;
}

/* Fit by measurement-free heuristic: long strings and long lists step down a
 * size band instead of overflowing the shape. */
function titleSize(value) {
	const len = text(value).length;
	if (len > 90) { return 24; }
	if (len > 60) { return 28; }
	if (len > 38) { return 32; }
	return 36;
}

function bulletSize(items) {
	const count = items.length;
	const longest = items.reduce(function (max, item) { return Math.max(max, text(item.text).length); }, 0);
	let size = count <= 4 ? 20 : (count <= 6 ? 18 : 16);
	if (longest > 120) { size -= 2; }
	if (longest > 200) { size -= 2; }
	return Math.max(size, 12);
}

/* bullets accept plain strings or { text, level, bold } */
function normalizeBullets(list) {
	if (!Array.isArray(list)) {
		return [];
	}
	return list
		.map(function (item) {
			if (isPlainObject(item)) {
				return {
					text: cleanBullet(item.text !== undefined ? item.text : item.label),
					level: Math.max(0, Math.min(4, parseInt(item.level, 10) || 0)),
					bold: !!item.bold
				};
			}
			return { text: cleanBullet(item), level: 0, bold: false };
		})
		.filter(function (item) { return item.text.trim() !== ''; });
}

function cleanBullet(value) {
	return text(value).replace(/^\s*(?:[\u2022\u25cf\u25e6\u00b7]\s*|[-*]\s+)/, '');
}

/* Never a literal bullet glyph in the string (that renders two bullets); the
 * list is one addText call with bullet:true per paragraph. */
function bulletParagraphs(items, size, color) {
	return items.map(function (item, index) {
		return {
			text: item.text,
			options: {
				fontSize: Math.max(12, size - item.level * 2),
				bold: item.bold,
				color: color,
				indentLevel: item.level,
				bullet: true,
				paraSpaceAfter: Math.max(6, size * 0.6),
				breakLine: index !== items.length - 1
			}
		};
	});
}

function addNotes(slide, spec) {
	const notes = text(spec.notes).trim();
	if (notes !== '') {
		slide.addNotes(notes);
	}
}

/* Shared chrome: title, footer and page number.
 * Options objects are built fresh every call - pptxgenjs converts them to EMU
 * in place, so a shared object silently corrupts the second use. */
function addTitle(slide, t, title, kicker) {
	const value = text(title).trim();
	if (value === '') {
		return;
	}
	if (text(kicker).trim() !== '') {
		slide.addText(text(kicker).trim().toUpperCase(), {
			x: MARGIN, y: TITLE_Y - 0.32, w: CONTENT_W, h: 0.3,
			fontFace: t.fonts.body, fontSize: 12, bold: true, charSpacing: 1.6, color: t.accent, margin: 0
		});
	}
	slide.addText(value, {
		x: MARGIN, y: TITLE_Y, w: CONTENT_W, h: TITLE_H,
		fontFace: t.fonts.heading, fontSize: titleSize(value), bold: true, color: t.ink,
		valign: 'top', margin: 0, fit: 'shrink'
	});
}

function addFooter(slide, t, label, index, total) {
	const caption = text(label).trim();
	if (caption !== '') {
		slide.addText(caption, {
			x: MARGIN, y: FOOTER_Y, w: CONTENT_W - 1.1, h: 0.32,
			fontFace: t.fonts.body, fontSize: 10, color: t.muted, margin: 0
		});
	}
	slide.addText(String(index) + (Number.isInteger(total) ? ' / ' + total : ''), {
		x: SLIDE_W - MARGIN - 1.0, y: FOOTER_Y, w: 1.0, h: 0.32,
		fontFace: t.fonts.body, fontSize: 10, color: t.muted, align: 'right', margin: 0
	});
}

/* ------------------------------ slide kinds ----------------------------- */

function renderTitleSlide(pres, t, spec) {
	const slide = pres.addSlide();
	slide.background = { color: t.bandBg };
	slide.addText(text(spec.title) || 'Untitled', {
		x: MARGIN + 0.2, y: 2.15, w: CONTENT_W - 0.4, h: 1.9,
		fontFace: t.fonts.heading, fontSize: text(spec.title).length > 42 ? 40 : 52, bold: true,
		color: t.invertInk, valign: 'bottom', margin: 0, fit: 'shrink'
	});
	if (text(spec.subtitle).trim() !== '') {
		slide.addText(text(spec.subtitle), {
			x: MARGIN + 0.2, y: 4.2, w: CONTENT_W - 0.4, h: 0.8,
			fontFace: t.fonts.body, fontSize: 22, color: t.accentSoft, margin: 0
		});
	}
	if (text(spec.footer).trim() !== '') {
		slide.addText(text(spec.footer), {
			x: MARGIN + 0.2, y: 5.25, w: CONTENT_W - 0.4, h: 0.5,
			fontFace: t.fonts.body, fontSize: 14, color: t.accentSoft, margin: 0
		});
	}
	addNotes(slide, spec);
	return slide;
}

function renderSection(pres, t, spec) {
	const slide = pres.addSlide();
	slide.background = { color: t.panel };
	slide.addText(text(spec.title) || '', {
		x: MARGIN + 0.25, y: 2.55, w: CONTENT_W - 0.5, h: 1.0,
		fontFace: t.fonts.heading, fontSize: 40, bold: true, color: t.ink, valign: 'middle', margin: 0, fit: 'shrink'
	});
	if (text(spec.subtitle).trim() !== '') {
		slide.addText(text(spec.subtitle), {
			x: MARGIN + 0.25, y: 3.6, w: CONTENT_W - 0.5, h: 0.6,
			fontFace: t.fonts.body, fontSize: 18, color: t.muted, margin: 0
		});
	}
	addNotes(slide, spec);
	return slide;
}

function renderBullets(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.bg };
	addTitle(slide, t, spec.title, spec.kicker);
	const items = normalizeBullets(spec.bullets || spec.points || spec.items);
	let y = BODY_Y;
	let h = BODY_H;
	if (text(spec.lead).trim() !== '') {
		slide.addText(text(spec.lead), {
			x: MARGIN, y: y, w: CONTENT_W, h: 0.75,
			fontFace: t.fonts.body, fontSize: 18, color: t.muted, margin: 0
		});
		y += 0.85;
		h -= 0.85;
	}
	if (items.length > 0) {
		const size = bulletSize(items);
		slide.addText(bulletParagraphs(items, size, t.ink, t.accent), {
			x: MARGIN, y: y, w: CONTENT_W, h: h, fontFace: t.fonts.body, valign: 'top', margin: 0, fit: 'shrink'
		});
	}
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

function renderTwoCol(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.bg };
	addTitle(slide, t, spec.title, spec.kicker);
	const gap = 0.45;
	const colW = (CONTENT_W - gap) / 2;
	const columns = [spec.left || {}, spec.right || {}];
	columns.forEach(function (column, i) {
		const x = MARGIN + i * (colW + gap);
		slide.addShape(pres.ShapeType.roundRect, {
			x: x, y: BODY_Y, w: colW, h: BODY_H - 0.2,
			fill: { color: t.panel }, line: { color: t.accentSoft, width: 1 }, rectRadius: 0.08
		});
		let inner = BODY_Y + 0.32;
		if (text(column.heading).trim() !== '') {
			slide.addText(text(column.heading), {
				x: x + 0.3, y: inner, w: colW - 0.6, h: 0.5,
				fontFace: t.fonts.body, fontSize: 20, bold: true, color: t.accent, margin: 0
			});
			inner += 0.62;
		}
		const items = normalizeBullets(column.bullets || column.points || column.items);
		if (items.length > 0) {
			const size = Math.min(bulletSize(items), 18);
			slide.addText(bulletParagraphs(items, size, t.ink, t.accent), {
				x: x + 0.3, y: inner, w: colW - 0.6, h: BODY_Y + BODY_H - 0.55 - inner,
				fontFace: t.fonts.body, valign: 'top', margin: 0, fit: 'shrink'
			});
		} else if (text(column.text).trim() !== '') {
			slide.addText(text(column.text), {
				x: x + 0.3, y: inner, w: colW - 0.6, h: BODY_Y + BODY_H - 0.55 - inner,
				fontFace: t.fonts.body, fontSize: 17, color: t.ink, valign: 'top', margin: 0, fit: 'shrink'
			});
		}
	});
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

function renderStats(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.bg };
	addTitle(slide, t, spec.title, spec.kicker);
	const stats = Array.isArray(spec.stats) ? spec.stats : [];
	if (stats.length > 6 || stats.some(function (stat) { return !isPlainObject(stat); })) {
		throw new Error('stat-row needs at most 6 stats, each with value and label. Split more stats across slides.');
	}
	const gap = 0.4;
	const cardW = stats.length > 0 ? (CONTENT_W - gap * (stats.length - 1)) / stats.length : CONTENT_W;
	const cardH = 2.5;
	const y = BODY_Y + 0.25;
	stats.forEach(function (stat, i) {
		const x = MARGIN + i * (cardW + gap);
		slide.addShape(pres.ShapeType.roundRect, {
			x: x, y: y, w: cardW, h: cardH,
			fill: { color: t.panel }, line: { color: t.accentSoft, width: 1 }, rectRadius: 0.08
		});
		slide.addText(text(stat.value), {
			x: x + 0.2, y: y + 0.35, w: cardW - 0.4, h: 1.0,
			fontFace: t.fonts.heading, fontSize: 40, bold: true, color: t.accent, align: 'center', margin: 0
		});
		slide.addText(text(stat.label), {
			x: x + 0.2, y: y + 1.45, w: cardW - 0.4, h: 0.55,
			fontFace: t.fonts.body, fontSize: 15, bold: true, color: t.ink, align: 'center', margin: 0
		});
		if (text(stat.caption).trim() !== '') {
			slide.addText(text(stat.caption), {
				x: x + 0.2, y: y + 1.95, w: cardW - 0.4, h: 0.45,
				fontFace: t.fonts.body, fontSize: 12, color: t.muted, align: 'center', margin: 0
			});
		}
	});
	const items = normalizeBullets(spec.bullets);
	if (items.length > 0) {
		slide.addText(bulletParagraphs(items, 15, t.muted), {
			x: MARGIN, y: y + cardH + 0.3, w: CONTENT_W, h: 1.1, fontFace: t.fonts.body, valign: 'top', margin: 0, fit: 'shrink'
		});
	}
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

function renderQuote(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.panel };
	addTitle(slide, t, spec.title, spec.kicker);
	slide.addText(text(spec.quote || spec.text), {
		x: MARGIN + 0.45, y: 2.1, w: CONTENT_W - 0.9, h: 2.0,
		fontFace: t.fonts.body, fontSize: 28, italic: true, color: t.ink, valign: 'top', margin: 0, fit: 'shrink'
	});
	if (text(spec.attribution).trim() !== '') {
		slide.addText(text(spec.attribution), {
			x: MARGIN + 0.45, y: 4.25, w: CONTENT_W - 0.9, h: 0.5,
			fontFace: t.fonts.body, fontSize: 16, bold: true, color: t.accent, margin: 0
		});
	}
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

function renderTable(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.bg };
	addTitle(slide, t, spec.title, spec.kicker);
	const columns = Array.isArray(spec.columns) ? spec.columns.map(text) : [];
	const rows = Array.isArray(spec.rows) ? spec.rows : [];
	const body = rows.map(function (row) {
		const cells = Array.isArray(row) ? row : [row];
		return cells.map(function (cell) {
			return { text: text(cell), options: { fontSize: 14, color: t.ink } };
		});
	});
	const table = [];
	if (columns.length > 0) {
		table.push(columns.map(function (heading) {
			return {
				text: heading,
				options: { bold: true, color: t.invertInk, fill: { color: t.accent }, fontSize: 14 }
			};
		}));
	}
	body.forEach(function (row) { table.push(row); });
	if (table.length > 0) {
		if (table.length > 10 || table.some(function (row) { return row.length !== table[0].length; })) {
			throw new Error('table needs equally sized rows and at most 10 rows including its header. Split larger tables across slides.');
		}
		slide.addTable(table, {
			x: MARGIN, y: BODY_Y, w: CONTENT_W,
			h: BODY_H - 0.2, rowH: (BODY_H - 0.2) / table.length, margin: 0.08,
			colW: undefined, fontFace: t.fonts.body, border: { pt: 1, color: t.accentSoft },
			fill: { color: t.bg }, valign: 'middle', autoPage: false
		});
	}
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

/* chart types come off the INSTANCE (pres.ChartType), never the module */
function chartTypeFor(pres, requested) {
	const name = text(requested).trim().toLowerCase() || 'bar';
	const types = pres.ChartType || {};
	const map = {
		bar: types.bar, column: types.bar, line: types.line, area: types.area,
		pie: types.pie, doughnut: types.doughnut, donut: types.doughnut,
		radar: types.radar
	};
	if (!Object.hasOwn(map, name) || !map[name]) {
		throw new Error('Unsupported chartType "' + name + '". Use bar, column, line, area, pie, doughnut or radar; use raw pptxgenjs for other types.');
	}
	return map[name];
}

function renderChart(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.bg };
	addTitle(slide, t, spec.title, spec.kicker);
	const categories = Array.isArray(spec.categories) ? spec.categories.map(text) : [];
	const series = (Array.isArray(spec.series) ? spec.series : []).map(function (s, i) {
		return {
			name: text(s && s.name ? s.name : 'Series ' + (i + 1)),
			labels: categories.slice(),
			values: (Array.isArray(s && s.values) ? s.values : []).map(function (v) {
				const n = Number(v);
				if (!Number.isFinite(n)) { throw new Error('chart series values must be finite numbers'); }
				return n;
			})
		};
	}).filter(function (s) { return s.values.length > 0; });
	const type = chartTypeFor(pres, spec.chartType || spec.type);
	const isPie = type === (pres.ChartType || {}).pie || type === (pres.ChartType || {}).doughnut;
	const isBar = type === (pres.ChartType || {}).bar;
	const grouping = spec.barGrouping || (spec.stacked ? 'stacked' : 'clustered');
	const stacked = isBar && ['stacked', 'percentStacked'].includes(grouping);
	const positions = isBar ? (stacked ? ['ctr', 'inEnd', 'inBase'] : ['ctr', 'inEnd', 'inBase', 'outEnd'])
		: (isPie ? ['ctr', 'inEnd', 'outEnd', 'bestFit'] : ['b', 'ctr', 'l', 'r', 't']);
	const labelPosition = positions.includes(spec.dataLabelPosition) ? spec.dataLabelPosition : (isBar ? 'inEnd' : 'ctr');
	if (series.length > 0) {
		if (categories.length === 0 || series.some(function (s) { return s.values.length !== categories.length; })) {
			throw new Error('chart needs categories and the same number of values in every series');
		}
		// defaults render bare (no labels, dated palette): set them explicitly
		slide.addChart(type, series, {
			x: MARGIN, y: BODY_Y, w: CONTENT_W, h: BODY_H - (spec.bullets && spec.bullets.length ? 1.0 : 0.35),
			chartColors: t.series.slice(),
			barDir: spec.barDir === 'bar' ? 'bar' : 'col',
			barGrouping: grouping,
			showTitle: !!text(spec.chartTitle).trim(), title: text(spec.chartTitle), titleFontFace: t.fonts.heading,
			showLegend: series.length > 1 || isPie,
			legendPos: 'b', legendColor: t.muted, legendFontSize: 12,
			showValue: !isPie, dataLabelPosition: labelPosition,
			dataLabelColor: t.ink, dataLabelFontSize: 11,
			showPercent: isPie,
			catAxisLabelColor: t.muted, valAxisLabelColor: t.muted,
			catAxisLabelFontSize: 12, valAxisLabelFontSize: 12,
			valGridLine: { color: t.accentSoft, size: 1 }, catGridLine: { style: 'none' },
			border: { pt: 0, color: t.bg }
		});
	} else {
		slide.addText('No chart data supplied', {
			x: MARGIN, y: BODY_Y, w: CONTENT_W, h: 0.6, fontFace: t.fonts.body, fontSize: 16, color: t.muted, margin: 0
		});
	}
	const items = normalizeBullets(spec.bullets);
	if (items.length > 0) {
		slide.addText(bulletParagraphs(items, 14, t.muted), {
			x: MARGIN, y: BODY_Y + BODY_H - 0.85, w: CONTENT_W, h: 0.7, fontFace: t.fonts.body, valign: 'top', margin: 0, fit: 'shrink'
		});
	}
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

function renderImage(pres, t, spec, index, total, footer) {
	const slide = pres.addSlide();
	slide.background = { color: t.bg };
	addTitle(slide, t, spec.title, spec.kicker);
	const frameY = text(spec.title).trim() === '' ? 0.7 : BODY_Y;
	const frameH = (text(spec.caption).trim() === '' ? BODY_H - 0.2 : BODY_H - 0.75)
		+ (text(spec.title).trim() === '' ? 0.9 : 0);
	// Geometry stays nonnegative. Mirror the image with flags, never signed w/h.
	const image = { x: MARGIN, y: frameY, w: CONTENT_W, h: frameH,
		flipH: !!spec.flipH, flipV: !!spec.flipV, sizing: { type: 'contain', w: CONTENT_W, h: frameH } };
	if (text(spec.imagePath).trim() !== '') {
		image.path = text(spec.imagePath);
	} else if (text(spec.imageData).trim() !== '') {
		// the "image/<type>;base64," prefix is required by pptxgenjs
		const data = text(spec.imageData);
		image.data = /^(?:data:)?image\/[a-z0-9.+-]+;base64,/i.test(data)
			? data.replace(/^data:/i, '') : 'image/png;base64,' + data;
	}
	if (image.path || image.data) {
		slide.addImage(image);
	} else {
		slide.addText('No image supplied', {
			x: MARGIN, y: frameY, w: CONTENT_W, h: 0.6, fontFace: t.fonts.body, fontSize: 16, color: t.muted, margin: 0
		});
	}
	if (text(spec.caption).trim() !== '') {
		slide.addText(text(spec.caption), {
			x: MARGIN, y: frameY + frameH + 0.12, w: CONTENT_W, h: 0.42,
			fontFace: t.fonts.body, fontSize: 13, color: t.muted, margin: 0
		});
	}
	addFooter(slide, t, footer, index, total);
	addNotes(slide, spec);
	return slide;
}

/* ---------------------- composable native authoring --------------------- */

// Clone before calling PptxGenJS: it mutates nested options and rich-text runs.
// Preserve native options we do not know about rather than maintaining a second
// allowlist of PptxGenJS features. Known invalid colors still fail early.
const SCHEME_COLORS = new Set(['tx1', 'tx2', 'bg1', 'bg2', 'dk1', 'dk2', 'lt1', 'lt2',
	'accent1', 'accent2', 'accent3', 'accent4', 'accent5', 'accent6', 'hlink', 'folHlink']);
function copyOptions(value, key) {
	if (typeof value === 'string' && (key === 'color' || /Color$/.test(key || '') || key === 'chartColors')) {
		return SCHEME_COLORS.has(value) ? value : hex(value);
	}
	if (Array.isArray(value)) {
		return value.map(function (item) { return copyOptions(item, key); });
	}
	if (value && Object.prototype.toString.call(value) === '[object Object]') {
		return Object.fromEntries(Object.entries(value).map(function (entry) {
			return [entry[0], copyOptions(entry[1], entry[0])];
		}));
	}
	return value;
}

function geometry(options, width, height) {
	const o = copyOptions(options || {});
	['x', 'y', 'w', 'h'].forEach(function (key) {
		if (o[key] === undefined) { return; }
		if (typeof o[key] === 'string' && /^-?\d+(?:\.\d+)?%$/.test(o[key])) {
			o[key] = parseFloat(o[key]) / 100 * (key === 'x' || key === 'w' ? width : height);
		}
		if (typeof o[key] !== 'number' || !Number.isFinite(o[key])) {
			throw new Error(key + ' must be a finite number in inches or a percentage');
		}
	});
	[['w', 'x', 'flipH'], ['h', 'y', 'flipV']].forEach(function (axis) {
		if (o[axis[0]] < 0) {
			o[axis[1]] = (o[axis[1]] || 0) + o[axis[0]];
			o[axis[0]] = -o[axis[0]];
			o[axis[2]] = !o[axis[2]];
		}
	});
	if (o.shadow && o.shadow.offset < 0) {
		o.shadow.offset = -o.shadow.offset;
		o.shadow.angle = ((o.shadow.angle || 0) + 180) % 360;
	}
	return o;
}

function safeChartLabels(type, options) {
	if ((type === 'bar' || type === 'bar3D') && ['stacked', 'percentStacked'].includes(options.barGrouping)
		&& options.dataLabelPosition && !['ctr', 'inEnd', 'inBase'].includes(options.dataLabelPosition)) {
		options.dataLabelPosition = 'inEnd';
	}
}

function guardSlide(slide, context) {
	slideContexts.set(slide, context);
	let objectNumber = 0;
	['addText', 'addShape', 'addImage', 'addChart', 'addTable'].forEach(function (method) {
		const original = slide[method].bind(slide);
		slide[method] = function () {
			const args = Array.from(arguments).map(function (arg) { return copyOptions(arg); });
			// Native combo charts accept (series, options), or (series, null, options).
			const chartOptionsAt = Array.isArray(args[0]) && (args[1] || args.length < 3) ? 1 : 2;
			const at = method === 'addImage' ? 0 : (method === 'addChart' ? chartOptionsAt : 1);
			const o = geometry(args[at], context.width, context.height);
			if (!o.objectName) { o.objectName = method.slice(3).toLowerCase() + '-' + (++objectNumber); }
			if (method === 'addText') {
				if (o.margin === undefined) { o.margin = 0; }
				if (Array.isArray(args[0])) {
					args[0].forEach(function (run) {
						if (run && run.options && run.options.bullet) { run.text = cleanBullet(run.text); }
					});
				} else if (o.bullet) { args[0] = cleanBullet(args[0]); }
			}
			if (method === 'addShape' && !args[0]) {
				throw new Error('Use the instance ShapeType, e.g. pres.ShapeType.rect');
			}
			if (method === 'addChart') {
				if (Array.isArray(args[0])) {
					args[0].forEach(function (series) {
						series.options = Object.assign({}, o, series.options);
						safeChartLabels(series.type, series.options);
					});
				} else { safeChartLabels(args[0], o); }
			}
			args[at] = o;
			return original.apply(null, args);
		};
	});
	// Native background assignment is an accessor, separate from the add* calls.
	let prototype = Object.getPrototypeOf(slide);
	while (prototype) {
		const property = Object.getOwnPropertyDescriptor(prototype, 'background');
		if (property && property.set) {
			Object.defineProperty(slide, 'background', {
				configurable: true,
				get: function () { return property.get && property.get.call(slide); },
				set: function (value) { property.set.call(slide, copyOptions(value)); }
			});
			break;
		}
		prototype = Object.getPrototypeOf(prototype);
	}
	return slide;
}

/** Return a native presentation. All slide.add* methods remain available. */
function create(args) {
	const o = args || {};
	if (typeof o.PptxGenJS !== 'function') {
		throw new Error('create() needs the injected constructor: deck.create({ PptxGenJS, theme: ... })');
	}
	const pres = new o.PptxGenJS();
	let width = SLIDE_W, height = SLIDE_H;
	if (o.width !== undefined || o.height !== undefined) {
		width = Number(o.width); height = Number(o.height);
		if (!(Number.isFinite(width) && width > 0 && Number.isFinite(height) && height > 0)) {
			throw new Error('A custom canvas needs positive width and height in inches');
		}
		pres.defineLayout({ name: 'SEMOSS_CUSTOM', width: width, height: height });
		pres.layout = 'SEMOSS_CUSTOM';
	} else { pres.layout = 'LAYOUT_WIDE'; }
	const theme = resolveTheme(o);
	pres.title = text(o.title);
	pres.subject = text(o.subject);
	pres.author = text(o.author) || 'SEMOSS';
	pres.company = text(o.company);
	pres.theme = { headFontFace: theme.fonts.heading, bodyFontFace: theme.fonts.body, lang: o.lang || 'en-US' };
	const context = { pres: pres, theme: theme, width: width, height: height };
	presentations.set(pres, context);
	const addSlide = pres.addSlide.bind(pres);
	pres.addSlide = function (options) {
		const slide = guardSlide(addSlide(copyOptions(options)), context);
		// Let a supplied master own its background.
		if (!options || !options.masterName) { slide.background = { color: theme.bg }; }
		return slide;
	};
	return pres;
}

function componentContext(slide, options) {
	const context = slideContexts.get(slide);
	if (!context) { throw new Error('Components need a slide from deck.create(...).addSlide()'); }
	if (options && options.theme !== undefined) {
		return Object.assign({}, context, { theme: resolveTheme({ theme: options.theme }) });
	}
	return context;
}

function frame(slide, options, defaults) {
	const c = componentContext(slide, options);
	// Accept both documented flat coordinates and a nested geometry object. Flat
	// coordinates take precedence; unknown nested keys fail instead of disappearing.
	const nested = options && options.geometry;
	if (nested !== undefined && (!nested || typeof nested !== 'object' || Array.isArray(nested)
		|| Object.keys(nested).some(key => !['x', 'y', 'w', 'h'].includes(key)))) {
		throw new Error('geometry must be an object containing only x, y, w and h');
	}
	return geometry(Object.assign({ x: MARGIN, y: BODY_Y, w: c.width - MARGIN * 2, h: c.height - BODY_Y - MARGIN },
		defaults, nested, options), c.width, c.height);
}

/** Equal cells in reading order. Use returned rectangles with any native API. */
function grid(options) {
	const o = Object.assign({ x: MARGIN, y: BODY_Y, w: CONTENT_W, h: BODY_H, columns: 2, rows: 1, gap: 0.4 }, options);
	if (![o.columns, o.rows].every(function (n) { return Number.isInteger(n) && n > 0; })
		|| ![o.x, o.y, o.w, o.h, o.gap].every(Number.isFinite) || o.gap < 0) {
		throw new Error('grid needs positive integer columns/rows, finite inch geometry and gap >= 0');
	}
	const w = (o.w - o.gap * (o.columns - 1)) / o.columns;
	const h = (o.h - o.gap * (o.rows - 1)) / o.rows;
	if (w <= 0 || h <= 0) { throw new Error('grid cells have no space; reduce gap or increase w/h'); }
	return Array.from({ length: o.rows * o.columns }, function (_, i) {
		return { x: o.x + i % o.columns * (w + o.gap), y: o.y + Math.floor(i / o.columns) * (h + o.gap), w: w, h: h };
	});
}

/** Theme-aware text. Explicit PptxGenJS options override the role defaults. */
function addText(slide, value, options) {
	const c = componentContext(slide, options), t = c.theme;
	const o = Object.assign({}, options);
	const role = o.role || 'body';
	const sizes = { title: 38, heading: 25, body: 19, caption: 13, value: 64 };
	delete o.role; delete o.theme;
	slide.addText(value, Object.assign({ x: MARGIN, y: BODY_Y, w: c.width - MARGIN * 2, h: 0.65,
		fontFace: ['title', 'heading', 'value'].includes(role) ? t.fonts.heading : t.fonts.body,
		fontSize: sizes[role] || sizes.body, color: role === 'caption' ? t.muted : t.ink,
		bold: ['title', 'heading', 'value'].includes(role), margin: 0, valign: 'top',
		objectName: role }, o));
	return slide;
}

function heading(slide, options) {
	const o = frame(slide, options, { y: 0.55, h: 1.15 });
	const t = componentContext(slide, o).theme;
	const titleY = o.y + (o.kicker ? 0.38 : 0);
	if (o.kicker) {
		addText(slide, text(o.kicker).toUpperCase(), { x: o.x, y: o.y, w: o.w, h: 0.25,
			fontSize: 12, bold: true, charSpacing: 1.5, color: o.accent || t.accent, objectName: 'heading kicker' });
	}
	addText(slide, text(o.title), { role: 'title', x: o.x, y: titleY, w: o.w, h: o.h,
		fontSize: o.fontSize || 38, fontFace: o.fontFace || t.fonts.heading, color: o.color || t.ink,
		objectName: 'slide title' });
	return slide;
}

function bullets(slide, items, options) {
	const o = frame(slide, options);
	const t = componentContext(slide, o).theme;
	const size = o.fontSize || 19;
	delete o.theme;
	slide.addText(bulletParagraphs(normalizeBullets(items), size, o.color || t.ink), Object.assign({
		fontFace: t.fonts.body, fontSize: size, margin: 0, valign: 'top', objectName: 'bullets'
	}, o));
	return slide;
}

function imageAspect(options) {
	if (typeof options.aspectRatio === 'number' && Number.isFinite(options.aspectRatio) && options.aspectRatio > 0) {
		return options.aspectRatio;
	}
	const data = options.path ? fs.readFileSync(options.path)
		: Buffer.from(text(options.data).replace(/^(?:data:)?image\/[^;,]+;base64,/i, ''), 'base64');
	let width = 0, height = 0;
	if (data.length >= 24 && data.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) {
		width = data.readUInt32BE(16); height = data.readUInt32BE(20);
	} else if (data.length >= 10 && /^GIF8[79]a/.test(data.toString('ascii', 0, 6))) {
		width = data.readUInt16LE(6); height = data.readUInt16LE(8);
	} else if (data.length >= 4 && data[0] === 0xff && data[1] === 0xd8) {
		let offset = 2;
		while (offset + 4 <= data.length) {
			if (data[offset++] !== 0xff) { break; }
			while (offset < data.length && data[offset] === 0xff) { offset++; }
			const marker = data[offset++];
			if (marker === 0xda || marker === 0xd9 || offset + 2 > data.length) { break; }
			if (marker === 0x01 || (marker >= 0xd0 && marker <= 0xd8)) { continue; }
			const length = data.readUInt16BE(offset);
			if (length < 2 || offset + length > data.length) { break; }
			if ([0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(marker) && length >= 7) {
				height = data.readUInt16BE(offset + 3); width = data.readUInt16BE(offset + 5); break;
			}
			offset += length;
		}
	} else {
		const svg = /<svg\b[^>]*>/i.exec(data.toString('utf8'));
		const viewBox = svg && /\bviewBox\s*=\s*["']\s*([-+.\deE]+)[\s,]+([-+.\deE]+)[\s,]+([-+.\deE]+)[\s,]+([-+.\deE]+)\s*["']/i.exec(svg[0]);
		if (viewBox) { width = Number(viewBox[3]); height = Number(viewBox[4]); }
		else if (svg) {
			const w = /\bwidth\s*=\s*["']([\d.]+)(?:px)?["']/i.exec(svg[0]);
			const h = /\bheight\s*=\s*["']([\d.]+)(?:px)?["']/i.exec(svg[0]);
			width = w && Number(w[1]); height = h && Number(h[1]);
		}
	}
	if (!(width > 0 && height > 0 && Number.isFinite(width / height))) {
		throw new Error('Cannot read image dimensions. Use PNG/JPEG/GIF/SVG, convert with curated sharp, or provide aspectRatio: width / height.');
	}
	return width / height;
}

/** Crop/contain an existing image in a rectangle without stretching it. */
function image(slide, options) {
	const o = frame(slide, options);
	const source = o.path || o.data;
	if (!source) { throw new Error('image needs path or data for an existing image'); }
	const mode = o.fit || 'cover';
	if (!['cover', 'contain', 'stretch'].includes(mode)) { throw new Error('image fit must be cover, contain or stretch'); }
	// PptxGenJS 4 uses sizing on addImage; older imageSizing* functions are
	// not present on the constructor or instance in the curated environment.
	if (mode !== 'stretch') {
		o.sizing = { type: mode, w: o.w, h: o.h };
		// The native sizing implementation uses addImage w/h as the SOURCE
		// aspect, and sizing.w/h as the TARGET box. Supplying the target for
		// both silently stretches images, even with sizing.type = 'cover'.
		o.h = o.w / imageAspect(o);
	}
	else { delete o.sizing; }
	delete o.fit; delete o.theme; delete o.aspectRatio;
	slide.addImage(Object.assign({ objectName: 'image' }, o));
	return slide;
}

/** A cover composition with optional image; every region remains editable. */
function cover(slide, options) {
	const c = componentContext(slide, options), t = c.theme;
	const o = Object.assign({}, options);
	const background = o.background === undefined ? { color: t.ink }
		: typeof o.background === 'string' ? { color: o.background } : o.background;
	if (!background || Array.isArray(background) || typeof background.color !== 'string' || !background.color.trim()) {
		throw new TypeError('deck.cover background must be a color string, e.g. "1A7F5A", or an object with color: "1A7F5A".');
	}
	slide.background = Object.assign({}, background);
	const x = o.x === undefined ? c.width * 0.065 : o.x;
	const y = o.y === undefined ? c.height * 0.25 : o.y;
	const w = o.w === undefined ? (o.image ? c.width * 0.49 : c.width - x * 2) : o.w;
	if (o.image) {
		image(slide, Object.assign({ x: c.width * 0.59, y: 0, w: c.width * 0.41, h: c.height }, o.image));
	}
	if (o.kicker) {
		addText(slide, text(o.kicker).toUpperCase(), { x: x, y: y - 0.5, w: w, h: 0.3,
			fontSize: 13, bold: true, charSpacing: 1.5, color: o.accent || t.accentSoft, objectName: 'cover kicker' });
	}
	addText(slide, text(o.title), { x: x, y: y, w: w, h: o.h || c.height * 0.35,
		role: 'title', fontSize: o.fontSize || 52, fontFace: o.fontFace || t.fonts.heading,
		color: o.color || t.invertInk, objectName: 'cover title' });
	if (o.subtitle) {
		addText(slide, text(o.subtitle), { x: x, y: y + (o.h || c.height * 0.35) + 0.25,
			w: w, h: 1.05, fontSize: o.subtitleSize || 21, color: o.subtitleColor || t.accentSoft, objectName: 'cover subtitle' });
	}
	if (o.footer) {
		addText(slide, text(o.footer), { x: x, y: c.height - 0.7, w: w, h: 0.3,
			role: 'caption', color: o.subtitleColor || t.accentSoft, objectName: 'cover footer' });
	}
	return slide;
}

function callout(slide, options) {
	const o = frame(slide, options, { w: 3.5, h: 2.4 });
	const t = componentContext(slide, o).theme;
	const valueH = o.h * 0.48;
	addText(slide, text(o.value), { role: 'value', x: o.x, y: o.y, w: o.w, h: valueH,
		fontSize: o.fontSize || Math.min(66, valueH * 58), color: o.color || t.accent,
		align: o.align || 'left', objectName: 'callout value' });
	addText(slide, text(o.label), { x: o.x, y: o.y + valueH + 0.08, w: o.w, h: o.h * 0.22,
		fontSize: o.labelSize || 20, bold: true, color: o.labelColor || t.ink,
		align: o.align || 'left', objectName: 'callout label' });
	if (o.caption) {
		addText(slide, text(o.caption), { x: o.x, y: o.y + o.h * 0.75, w: o.w, h: o.h * 0.25,
			fontSize: o.captionSize || 16, color: o.captionColor || t.muted,
			align: o.align || 'left', objectName: 'callout caption' });
	}
	return slide;
}

function comparison(slide, options) {
	const o = frame(slide, options);
	const t = componentContext(slide, o).theme;
	const cells = grid({ x: o.x, y: o.y, w: o.w, h: o.h, columns: 2, gap: o.gap === undefined ? 0.8 : o.gap });
	[o.left || {}, o.right || {}].forEach(function (column, i) {
		const cell = cells[i];
		addText(slide, text(column.heading), { role: 'heading', x: cell.x, y: cell.y, w: cell.w, h: 0.65,
			fontSize: column.headingSize || o.headingSize || 26, color: column.color || t.accent,
			objectName: 'comparison ' + (i ? 'right' : 'left') + ' heading' });
		const body = { x: cell.x, y: cell.y + 0.9, w: cell.w, h: cell.h - 0.9,
			fontSize: o.fontSize || 20, color: t.ink, objectName: 'comparison ' + (i ? 'right' : 'left') + ' body' };
		if (column.bullets) { bullets(slide, column.bullets, body); }
		else { addText(slide, text(column.text), body); }
	});
	return slide;
}

/** Choose readable marker ink from the marker fill, independently of slide background. */
function contrastingInk(color) {
	const rgb = hex(color, 'FFFFFF');
	const channels = [0, 2, 4].map(function (offset) {
		const c = parseInt(rgb.slice(offset, offset + 2), 16) / 255;
		return c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
	});
	const luminance = 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
	return (luminance + 0.05) / 0.05 >= 1.05 / (luminance + 0.05) ? '000000' : 'FFFFFF';
}

/** Editable horizontal timeline or vertical process, with optional dates. */
function timeline(slide, options) {
	const o = frame(slide, options, { h: 3.8 });
	const c = componentContext(slide, o), t = c.theme;
	if (!Array.isArray(o.steps) || !o.steps.length) { throw new Error('timeline needs a nonempty steps array'); }
	const vertical = o.direction === 'vertical';
	const cells = grid({ x: o.x, y: o.y, w: o.w, h: o.h,
		columns: vertical ? 1 : o.steps.length, rows: vertical ? o.steps.length : 1,
		gap: o.gap === undefined ? (vertical ? 0.2 : 0.35) : o.gap });
	const color = o.color || t.accent;
	if (cells.length > 1) {
		const last = cells[cells.length - 1];
		slide.addShape(c.pres.ShapeType.line, { x: o.x + 0.22, y: o.y + 0.22,
			w: vertical ? 0 : last.x - o.x, h: vertical ? last.y - o.y : 0,
			line: { color: t.accentSoft, width: 2 }, objectName: 'timeline connector' });
	}
	o.steps.forEach(function (step, i) {
		const item = typeof step === 'string' ? { title: step } : step;
		if (!item || !text(item.title).trim()) { throw new Error('Each timeline step needs a title'); }
		const box = cells[i], tx = box.x + (vertical ? 0.68 : 0), ty = box.y + (vertical ? 0 : 0.85);
		slide.addShape(c.pres.ShapeType.ellipse, { x: box.x, y: box.y, w: 0.44, h: 0.44,
			fill: { color: color }, line: { color: color, transparency: 100 }, objectName: 'timeline marker ' + (i + 1) });
		addText(slide, String(i + 1), { x: box.x, y: box.y + 0.035, w: 0.44, h: 0.35,
			fontSize: 14, color: o.markerTextColor || contrastingInk(color), bold: true, align: 'center', objectName: 'timeline number ' + (i + 1) });
		const w = box.w - (vertical ? 0.68 : 0);
		const labelH = item.label ? 0.32 : 0;
		if (item.label) {
			addText(slide, text(item.label), { x: tx, y: ty, w: w, h: labelH, fontSize: 13,
				bold: true, color: o.labelColor || t.ink, objectName: 'timeline label ' + (i + 1) });
		}
		addText(slide, text(item.title), { x: tx, y: ty + labelH, w: w, h: vertical ? 0.4 : 0.8,
			fontSize: o.fontSize || (vertical ? 20 : 23), bold: true, color: o.titleColor || t.ink, objectName: 'timeline title ' + (i + 1) });
		if (item.text) {
			const y = ty + labelH + (vertical ? 0.48 : 0.96);
			addText(slide, text(item.text), { x: tx, y: y, w: w, h: Math.max(0.2, box.y + box.h - y),
				fontSize: o.bodySize || 18, color: o.bodyColor || t.muted, objectName: 'timeline detail ' + (i + 1) });
		}
	});
	return slide;
}

/** Styled native chart; options can override any PptxGenJS chart property. */
function chart(slide, options) {
	const o = frame(slide, options);
	const c = componentContext(slide, o), t = c.theme;
	const categories = Array.isArray(o.categories) ? o.categories.map(text) : [];
	if (!categories.length || !Array.isArray(o.series) || !o.series.length) {
		throw new Error('chart needs categories and at least one series');
	}
	const data = o.series.map(function (series) {
		if (!series || !Array.isArray(series.values) || series.values.length !== categories.length
			|| !series.values.every(function (v) { return typeof v === 'number' && Number.isFinite(v); })) {
			throw new Error('Each chart series needs one finite number per category');
		}
		return { name: text(series.name), labels: categories.slice(), values: series.values.slice() };
	});
	const type = o.type === 'column' ? c.pres.ChartType.bar : (o.type || c.pres.ChartType.bar);
	const pie = type === c.pres.ChartType.pie || type === c.pres.ChartType.doughnut;
	const native = Object.assign({ x: o.x, y: o.y, w: o.w, h: o.h, chartColors: t.series.slice(),
		showLegend: pie || data.length > 1, legendPos: 'b', legendColor: t.muted, legendFontSize: 13,
		showTitle: false, showValue: !pie, showPercent: pie,
		dataLabelColor: t.ink, dataLabelFontSize: 14,
		dataLabelPosition: type === c.pres.ChartType.bar ? 'outEnd' : (pie ? 'bestFit' : 't'),
		catAxisLabelColor: t.muted, valAxisLabelColor: t.muted, catAxisLabelFontSize: 13, valAxisLabelFontSize: 13,
		catGridLine: { style: 'none' }, valGridLine: { color: t.accentSoft, size: 0.5 },
		showBorder: false, showCatName: false, barDir: o.type === 'bar' ? 'bar' : 'col',
		objectName: 'chart' }, o.options);
	slide.addChart(type, data, native);
	return slide;
}

/** Add one legacy layout to a native deck and return it for customization. */
function addLayout(pres, spec) {
	const context = presentations.get(pres);
	if (!context) { throw new Error('addLayout needs a presentation from deck.create(...)'); }
	if (context.width !== SLIDE_W || context.height !== SLIDE_H) {
		throw new Error('Original JSON layouts use the wide canvas. Use components or native calls on a custom canvas.');
	}
	const renderers = { title: renderTitleSlide, section: renderSection, bullets: renderBullets,
		'two-col': renderTwoCol, 'stat-row': renderStats, quote: renderQuote, table: renderTable,
		chart: renderChart, image: renderImage };
	const kind = spec && (spec.kind || spec.layout);
	if (!renderers[kind]) { throw new Error('addLayout kind must be one of: ' + KINDS.join(', ')); }
	return renderers[kind](pres, resolveTheme({ theme: spec.theme || context.theme }), spec,
		pres.slides.length + 1, undefined, text(spec.footer));
}

/** Save a native presentation; count is checked before replacing the output. */
async function save(pres, outPath, options) {
	if (!presentations.has(pres)) { throw new Error('save needs a presentation from deck.create(...)'); }
	if (typeof outPath !== 'string' || !path.isAbsolute(outPath)) {
		throw new Error('outPath must be absolute: use path.join(ROOT, "<exact filename including extension>")');
	}
	const count = pres.slides.length;
	if (!count || (options && options.slides !== undefined && options.slides !== count)) {
		throw new Error('Presentation has ' + count + ' slides; match the requested count before saving');
	}
	if (!pres.subject || /^SEMOSS presentation: \d+ slides$/.test(pres.subject)) {
		pres.subject = 'SEMOSS presentation: ' + count + ' slides';
	}
	fs.mkdirSync(path.dirname(outPath), { recursive: true });
	await pres.writeFile({ fileName: outPath });
	renderedSlideCounts.set(path.resolve(outPath), count);
	return { file: outPath, slides: count, bytes: fs.statSync(outPath).size };
}

/* -------------------------------- render -------------------------------- */

/**
 * Render a deck spec to a .pptx file.
 *
 * @param {object} args
 * @param {Function} args.PptxGenJS the constructor from require('pptxgenjs')
 * @param {object} args.spec deck spec: { title, subtitle, theme, footer, slides: [...] }
 * @param {string} args.outPath absolute output path (use path.join(ROOT, name))
 * @returns {Promise<object>} { file, slides, bytes }
 */
async function render(args) {
	const options = args || {};
	const PptxGenJS = options.PptxGenJS || options.pptxgenjs || options.ctor;
	if (typeof PptxGenJS !== 'function') {
		throw new Error('render() needs PptxGenJS: pass the constructor, e.g. '
			+ 'deck.render({ PptxGenJS: require("pptxgenjs"), spec: ..., outPath: ... })');
	}
	const spec = options.spec;
	if (!isPlainObject(spec)) {
		throw new Error('render() needs a spec object with a slides array');
	}
	const outPath = text(options.outPath || options.file).trim();
	if (outPath === '') {
		throw new Error('render() needs outPath - use path.join(ROOT, "<filename>.pptx")');
	}
	if (!path.isAbsolute(outPath)) {
		throw new Error('outPath must be absolute: use path.join(ROOT, "<exact filename>.pptx")');
	}
	const slides = Array.isArray(spec.slides) ? spec.slides : [];
	if (slides.length === 0) {
		throw new Error('spec.slides is empty - each entry needs a kind, one of: ' + KINDS.join(', '));
	}
	if (slides.some(function (slide) { return !isPlainObject(slide); })) {
		throw new Error('every spec.slides entry must be an object; no slides were omitted');
	}

	const t = resolveTheme(spec);
	const pres = new PptxGenJS();
	// layout must be set BEFORE the first addSlide
	pres.layout = 'LAYOUT_WIDE';
	pres.title = text(spec.title) || 'Presentation';
	pres.author = text(spec.author) || 'SEMOSS';
	pres.company = text(spec.company) || '';
	// Persist the spec count for validate() in a fresh process as well.
	pres.subject = 'SEMOSS JSON deck spec: ' + slides.length + ' slides';
	pres.theme = { headFontFace: t.fonts.heading, bodyFontFace: t.fonts.body, lang: 'en-US' };

	const footer = text(spec.footer !== undefined ? spec.footer : spec.title);
	slides.forEach(function (slideSpec, i) {
		const kind = text(slideSpec.kind || slideSpec.layout || 'bullets').trim().toLowerCase();
		const position = i + 1;
		switch (kind) {
			case 'title':
			case 'cover':
				renderTitleSlide(pres, t, slideSpec);
				break;
			case 'section':
			case 'divider':
				renderSection(pres, t, slideSpec);
				break;
			case 'bullets':
			case 'content':
				renderBullets(pres, t, slideSpec, position, slides.length, footer);
				break;
			case 'two-col':
			case 'two_col':
			case 'twocol':
			case 'compare':
				renderTwoCol(pres, t, slideSpec, position, slides.length, footer);
				break;
			case 'stats':
			case 'stat-row':
			case 'kpi':
				renderStats(pres, t, slideSpec, position, slides.length, footer);
				break;
			case 'quote':
				renderQuote(pres, t, slideSpec, position, slides.length, footer);
				break;
			case 'table':
				renderTable(pres, t, slideSpec, position, slides.length, footer);
				break;
			case 'chart':
				renderChart(pres, t, slideSpec, position, slides.length, footer);
				break;
			case 'image':
				renderImage(pres, t, slideSpec, position, slides.length, footer);
				break;
			default:
				throw new Error('Unknown slide kind "' + kind + '" at slides[' + i + ']. Valid kinds: '
					+ KINDS.join(', '));
		}
	});

	fs.mkdirSync(path.dirname(path.resolve(outPath)), { recursive: true });
	await pres.writeFile({ fileName: outPath });
	const stat = fs.statSync(outPath);
	renderedSlideCounts.set(path.resolve(outPath), slides.length);
	return { file: outPath, slides: slides.length, bytes: stat.size };
}

/* ------------------------------- validate ------------------------------- */
/** Return { ok, file, slides, expectedSlides, checks, errors, warnings }.
 * An optional { slides: count } checks the caller's requested slide count too.
 * The file's embedded spec count survives module reloads and worker restarts.
 */
function validate(file, expect) {
	const count = typeof file === 'string' ? renderedSlideCounts.get(path.resolve(file)) : undefined;
	return validatePackage(file, Object.assign({}, count === undefined ? {} : { slides: count, pptxgenjs: true }, expect));
}

module.exports = {
	create: create,
	save: save,
	text: addText,
	heading: heading,
	bullets: bullets,
	image: image,
	cover: cover,
	callout: callout,
	comparison: comparison,
	timeline: timeline,
	chart: chart,
	grid: grid,
	addLayout: addLayout,
	render: render,
	validate: validate,
	KINDS: KINDS,
	THEMES: Object.keys(THEMES),
	SLIDE_W: SLIDE_W,
	SLIDE_H: SLIDE_H
};
