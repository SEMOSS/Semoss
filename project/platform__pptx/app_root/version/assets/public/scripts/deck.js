'use strict';
/*
 * deck.js - JSON-spec deck builder for the SEMOSS agent Node environment.
 *
 * Why this exists: writing pptxgenjs by hand costs many turns and hits the same
 * footguns every time (negative extents, "#" colors, module-vs-instance
 * ShapeType/ChartType, bullets rendered twice, text off the canvas). Describe
 * the deck as data instead and this renders it, then validates the file.
 *
 * Usage from ExecuteNodeCode - ONE call, everything inside one async IIFE:
 *
 *   (async () => {
 *     const path = require('path');
 *     const PptxGenJS = require('pptxgenjs');
 *     const deck = require(path.join(ROOT, '.claude/skills/pptx/scripts/deck.js'));
 *     const out = path.join(ROOT, 'my-deck.pptx');
 *     const built = await deck.render({
 *       PptxGenJS: PptxGenJS,
 *       outPath: out,
 *       spec: {
 *         title: 'Quarterly Review',
 *         theme: 'navy',
 *         slides: [
 *           { kind: 'title',   title: 'Quarterly Review', subtitle: 'FY26 Q3' },
 *           { kind: 'bullets', title: 'Highlights', bullets: ['Revenue up 12%', 'Churn flat'] },
 *           { kind: 'stat-row', title: 'By the numbers',
 *             stats: [{ value: '12%', label: 'Revenue growth' }, { value: '1.4M', label: 'Active users' }] },
 *           { kind: 'two-col', title: 'Risks and mitigations',
 *             left:  { heading: 'Risks', bullets: ['Supply delays'] },
 *             right: { heading: 'Mitigations', bullets: ['Second supplier'] } },
 *           { kind: 'chart', title: 'Trend', chartType: 'bar',
 *             categories: ['Q1', 'Q2', 'Q3'], series: [{ name: 'Revenue', values: [8, 10, 12] }] }
 *         ]
 *       }
 *     });
 *     console.log(JSON.stringify(built));
 *     console.log(JSON.stringify(deck.validate(out), null, 1));
 *   })()
 *
 * PptxGenJS is injected because this file lives in the room's skill folder,
 * outside the curated node_env: a bare require('pptxgenjs') from here does not
 * resolve. Only node core modules are required internally.
 *
 * Fixed layouts own all geometry; spec data cannot inject raw pptxgenjs
 * options. Unknown kinds or data that cannot fit fail with an actionable error.
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
	slide.addText(String(index) + ' / ' + String(total), {
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
	render: render,
	validate: validate,
	KINDS: KINDS,
	THEMES: Object.keys(THEMES),
	SLIDE_W: SLIDE_W,
	SLIDE_H: SLIDE_H
};
