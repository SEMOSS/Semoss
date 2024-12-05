package prerna.theme;

import java.io.IOException;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.theme.BlocksThemeUtils;

public abstract class AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(AbstractThemeUtils.class);

	static boolean initialized = false;
	static RDBMSNativeEngine themeDb;
	
	/**
	 * Only used for static references
	 */
	AbstractThemeUtils() {
		
	}
	
	public static void loadThemingDatabase() throws Exception {
		themeDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.THEMING_DB);
		ThemeOwlCreator owlCreator = new ThemeOwlCreator(themeDb);
		if(owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		initialize();
		initialized = true;
	}
	
	private static void initialize() throws SQLException {
		String[] adminThemeColNames = null;
		String[] adminThemeTypes = null;
		String[] blocksTemplateColNames = null;
		String[] blocksTemplateTypes = null;
		/*
		 * Currently used
		 */
		
		// ADMIN_THEME
		AbstractSqlQueryUtil queryUtil = themeDb.getQueryUtil();
		
		adminThemeColNames = new String[] { "ID", "THEME_NAME", "THEME_MAP", "IS_ACTIVE" };
		adminThemeTypes = new String[] { "varchar(255)", "varchar(255)", queryUtil.getClobDataTypeName(), queryUtil.getBooleanDataTypeName() };
		if(queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(), adminThemeColNames, adminThemeTypes));
		} else {
			if(!queryUtil.tableExists(themeDb.getConnection(), ThemeDbTable.ADMIN_THEME.getThemeDbTableName(), themeDb.getDatabase(), themeDb.getSchema())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(), adminThemeColNames, adminThemeTypes));
			}
		}
		
		// BLOCKS_TEMPLATE
		
		blocksTemplateColNames = new String[] { "ID", "NAME", "SECTION", "IMAGE", "HOVER_IMAGE", "BLOCK_JSON" , "CLASSIFICATION", "IS_DELETABLE", "DATE_ADDED", "IS_LATEST"};
		blocksTemplateTypes = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", queryUtil.getClobDataTypeName(), "varchar(255)", queryUtil.getBooleanDataTypeName(), queryUtil.getDateWithTimeDataType(), queryUtil.getBooleanDataTypeName() };
		if(queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes));
		} else {
			if(!queryUtil.tableExists(themeDb.getConnection(), ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), themeDb.getDatabase(), themeDb.getSchema())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes));
				populateBlocksTemplateTable(blocksTemplateColNames, blocksTemplateTypes, queryUtil);
			}
		}
			if (!BlocksThemeUtils.getBlockNames().containsAll(BlocksThemeUtils.BASE_BLOCKS)) {
				populateBlocksTemplateTable(blocksTemplateColNames, blocksTemplateTypes, queryUtil);
			}

		// commit the changes
		themeDb.commit();
	}

	private static void populateBlocksTemplateTable(String[] blocksTemplateColNames, String[] blocksTemplateTypes,
		AbstractSqlQueryUtil queryUtil) throws SQLException {
		
			//delete the contents of the table
			themeDb.removeData("DELETE FROM " + ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName());
	
			// insert default blocks
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
					new Object[] {
					    "BT001", "Audio Player", "SECTION_INPUT", "BLOCK_AUDIO_PLAYER", "BLOCK_AUDIO_PLAYER",
				"{widget: 'audio-player', data: {label: 'Audio Player', autoplay: false, controls: true, loop: false, source: ''}, listeners: {}, slots: {}}", "DEFAULT", false, Utility.getCurrentSqlTimestampUTC(), true
					    })
					);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT002", "Button", "SECTION_INPUT", "BLOCK_BUTTON", "BLOCK_BUTTON",
				"{widget: 'button', data: {style: {}, label: 'Submit', loading: false, disabled: false, variant: 'contained', color: 'primary'}, listeners: {onClick: []}, slots: {}}", "DEFAULT", 
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT003", "Checkbox", "SECTION_INPUT", "BLOCK_CHECKBOX", "BLOCK_CHECKBOX",
				"{widget: 'checkbox', data: {style: {padding: 'none'}, label: 'Example Checkbox', required: false, disabled: false, value: false}, listeners: {onChange: []}, slots: {}}", "DEFAULT", 
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT004", "Input", "SECTION_INPUT", "BLOCK_INPUT", "BLOCK_INPUT",
				"{widget: 'input', data: {style: {width: '100%', padding: '4px'}, value: '', label: 'Example Input', hint: '', type: 'text', rows: 1, multiline: false, disabled: false, required: false, loading: false}, listeners: {onChange: []}, slots: {content: []}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT005", "Select", "SECTION_INPUT", "BLOCK_SELECT", "BLOCK_SELECT",
				"{widget: 'select', data: {style: {padding: '4px'}, value: '', label: 'Example Select Input', hint: '', options: [], required: false, disabled: false, loading: false}, listeners: {onChange: []}, slots: {content: []}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT006", "Upload", "SECTION_INPUT", "BLOCK_FILE_UPLOAD", "BLOCK_FILE_UPLOAD",
				"{widget: 'upload', data: {style: {width: '100%', padding: '4px'}, value: '', label: 'Example Input', hint: '', loading: false, disabled: false, required: false}, listeners: {onChange: []}, slots: {content: []}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT007", "Container", "SECTION_LAYOUT", "BLOCK_CONTAINER", "BLOCK_CONTAINER",
				"{widget: 'container', data: {style: {display: 'flex', flexDirection: 'column', padding: '4px', gap: '8px', flexWrap: 'wrap'}}, listeners: {}, slots: {children: []}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT008", "Progress", "SECTION_PROGRESS", "BLOCK_PROGRESS_BAR", "BLOCK_PROGRESS_BAR",
				"{widget: 'progress', data: {type: 'linear', value: 50, includeLabel: true, size: '300px'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT009", "Iframe", "SECTION_ELEMENT", "BLOCK_IFRAME", "BLOCK_IFRAME",
				"{widget: 'iframe', data: {style: {}, src: '', title: '', enableFrameInteractions: true}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT010", "PDF Viewer", "SECTION_ELEMENT", "BLOCK_PDF_VIEWER", "BLOCK_PDF_VIEWER",
				"{widget: 'pdfViewer', data: {style: {width: '100%', height: '82%', padding: '8px'}, selectedPdf: null}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT011", "Image", "SECTION_ELEMENT", "BLOCK_IMAGE", "BLOCK_IMAGE",
				"{widget: 'image', data: {style: {display: 'flex', justifyContent: 'center', alignItems: 'center', width: '100%', height: '200px', backgroundSize: 'contain', backgroundRepeat: 'no-repeat', backgroundPosition: 'center center'}, src: '', title: ''}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT012", "Logs", "SECTION_TEXT", "BLOCK_LOG", "BLOCK_LOG",
				"{widget: 'logs', data: {style: {}, queryId: ''}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT013", "Toggle Button", "SECTION_INPUT", "BLOCK_TOGGLE_BUTTON", "BLOCK_TOGGLE_BUTTON",
				"{widget: 'toggle-button', data: {disabled: false, color: 'primary', size: 'small', options: [{display: 'on', value: 'on'}, {display: 'off', value: 'off'}], value: null, mandatory: true, multiple: false}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT014", "Link", "SECTION_TEXT", "BLOCK_LINK", "BLOCK_LINK",
				"{widget: 'link', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, href: '', text: 'Insert text'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT015", "Markdown", "SECTION_TEXT", "BLOCK_MARKDOWN", "BLOCK_MARKDOWN",
				"{widget: 'markdown', data: {style: {padding: '4px'}, markdown: '**Hello world**'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT016", "HTML", "SECTION_ELEMENT", "HTML_BLOCK", "HTML_BLOCK",
				"{widget: 'html', data: {style: {padding: '4px'}, html: '<html>\\r\\n <style>\\r\\n html {\\r\\n font-family: Roboto;\\r\\n text-align: center;\\r\\n overflow: hidden;\\r\\n}\\r\\n </style>\\r\\n <body>\\r\\n <h2>HTML Block</h2>\\r\\n </body>\\r\\n</html>'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT017", "Text H1 styled", "SECTION_TEXT", "BLOCK_H1_STYLED", "BLOCK_H1_STYLED",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis', color: 'rgb(0,76,255)', fontFamily: 'Times New Roman'}, text: 'Hello world', variant: 'h1'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT018", "Text H1", "SECTION_TEXT", "BLOCK_H1", "BLOCK_H1",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h1'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT019", "Text H2", "SECTION_TEXT", "BLOCK_H2", "BLOCK_H2",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h2'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT020", "Text H3", "SECTION_TEXT", "BLOCK_H3", "BLOCK_H3",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h3'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
					    new Object[] {
					        "BT021", "Text H4", "SECTION_TEXT", "BLOCK_H4", "BLOCK_H4",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h4'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT022", "Text H5", "SECTION_TEXT", "BLOCK_H5", "BLOCK_H5",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h5'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT023", "Text H6", "SECTION_TEXT", "BLOCK_H6", "BLOCK_H6",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h6'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT024", "Text P", "SECTION_TEXT", "BLOCK_P", "BLOCK_P",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'p'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT025", "Text P Italics", "SECTION_TEXT", "BLOCK_P_ITALICS", "BLOCK_P_ITALICS",
				"{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis', fontStyle: 'italic'}, text: 'Hello world', variant: 'p'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT026", "Compare LLMs", "SECTION_COMPARE_LLMS", null, null,
				"{widget: 'llmComparison', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: '', variants: {}}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT027", "Mermaid", "SECTION_MERMAID", "BLOCK_MERMAID", "BLOCK_MERMAID",
				"{widget: 'mermaid', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Query', variant: 'p'}, listeners: {}, slots: {}}", "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				// Insert visualization blocks
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT028", "Vega", "SECTION_GENERAL_VISUALIZATION", "GENERAL_CHART", "GENERAL_CHART",
				"{widget: 'vega', data: {specJson: '', variation: undefined}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT029", "Grid", "SECTION_GENERAL_VISUALIZATION", "GRID", "GRID",
				"{widget: 'grid', data: {frame: {name: ''}, columns: [], view: {pagination: true}}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT030", "Bar Chart", "SECTION_BAR_CHART", "BAR_CHART", "BAR_CHART",
				"{widget: 'vega', data: {variation: 'bar-chart', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Bar Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'C', b: 43}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'F', b: 53}, {a: 'G', b: 19}, {a: 'H', b: 87}, {a: 'I', b: 52}]}, mark: 'bar', encoding: {x: {field: 'a', type: 'ordinal'}, y: {field: 'b', type: 'quantitative'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT031", "Grouped Bar Chart", "SECTION_BAR_CHART", "GROUP_BAR_CHART", "GROUP_BAR_CHART",
				"{widget: 'vega', data: {variation: 'grouped-bar-chart', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Grouped Bar Chart', width: 300, height: 300, data: {values: [{category: 'A', group: 'x', value: 0.1}, {category: 'A', group: 'y', value: 0.6}, {category: 'A', group: 'z', value: 0.9}, {category: 'B', group: 'x', value: 0.7}, {category: 'B', group: 'y', value: 0.2}, {category: 'B', group: 'z', value: 1.1}, {category: 'C', group: 'x', value: 0.6}, {category: 'C', group: 'y', value: 0.1}, {category: 'C', group: 'z', value: 0.2}]}, mark: 'bar', encoding: {x: {field: 'category'}, y: {field: 'value', type: 'quantitative'}, xOffset: {field: 'group'}, color: {field: 'group'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT032", "Pie Chart", "SECTION_PIE_CHART", "PIE_CHART_IMAGE", "PIE_CHART_IMAGE",
				"{widget: 'vega', data: {variation: 'pie-chart', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Pie Chart', width: 300, height: 300, description: 'A simple pie chart with embedded data.', data: {values: [{category: 1, value: 4}, {category: 2, value: 6}, {category: 3, value: 10}, {category: 4, value: 3}, {category: 5, value: 7}, {category: 6, value: 8}]}, mark: 'arc', encoding: {theta: {field: 'value', type: 'quantitative'}, color: {field: 'category', type: 'nominal'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT033", "Radial Plot", "SECTION_PIE_CHART", "RADIAL_CHART", "RADIAL_CHART",
				"{widget: 'vega', data: {variation: 'radial-plot', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Radial Plot', width: 300, height: 300, description: 'A simple radial chart with embedded data.', data: {values: [12, 23, 47, 6, 52, 19]}, layer: [{mark: {type: 'arc', innerRadius: 20, stroke: '#fff'}}, {mark: {type: 'text', radiusOffset: 10}, encoding: {text: {field: 'data', type: 'quantitative'}}}], encoding: {theta: {field: 'data', type: 'quantitative', stack: true}, radius: {field: 'data', scale: {type: 'sqrt', zero: true, rangeMin: 20}}, color: {field: 'data', type: 'nominal', legend: null}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT034", "Line Chart", "SECTION_LINE_CHART", "LINE_CHART", "LINE_CHART",
				"{widget: 'vega', data: {variation: 'line-chart', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Line Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55, predicted: false}, {a: 'D', b: 91, predicted: false}, {a: 'E', b: 81, predicted: false}, {a: 'E', b: 81, predicted: true}, {a: 'G', b: 19, predicted: true}, {a: 'H', b: 87, predicted: true}]}, mark: 'line', encoding: {x: {field: 'a', type: 'ordinal'}, y: {field: 'b', type: 'quantitative'}, strokeDash: {field: 'predicted', type: 'nominal'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT035", "Area Chart", "SECTION_AREA_CHART", "AREA_CHART", "AREA_CHART",
			    "{widget: 'vega', data: {variation: 'area-chart', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Area Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'E', b: 81}, {a: 'G', b: 19}, {a: 'H', b: 87}]}, mark: 'area', encoding: {x: {field: 'a'}, y: {aggregate: 'sum', field: 'b', title: 'count'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT036", "Area Chart with Gradient", "SECTION_AREA_CHART", "GRADIENT_CHART", "GRADIENT_CHART",
				"{widget: 'vega', data: {variation: 'area-chart-with-gradient', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Area Chart with Gradient', width: 300, height: 300, description: 'Simple area chart with gradient.', data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'E', b: 81}, {a: 'G', b: 19}, {a: 'H', b: 87}]}, mark: {type: 'area', line: {color: 'darkgreen'}, color: {x1: 1, y1: 1, x2: 1, y2: 0, gradient: 'linear', stops: [{offset: 0, color: 'white'}, {offset: 1, color: 'darkgreen'}]}}, encoding: {x: {field: 'a'}, y: {aggregate: 'sum', field: 'b', title: 'count'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT037", "Scatter Plot", "SECTION_SCATTER_PLOTS", "SCATTER_PLOT", "SCATTER_PLOT",
				"{widget: 'vega', data: {variation: 'scatter-plot', specJson: JSON.stringify({$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Scatter Plot', width: 300, height: 300, description: 'A scatterplot.', data: {values: [{a: 10, b: 28}, {a: 20, b: 55}, {a: 30, b: 91}, {a: 40, b: 81}, {a: 50, b: 81}, {a: 60, b: 19}, {a: 70, b: 87}]}, mark: 'point', encoding: {x: {field: 'a', type: 'quantitative'}, y: {field: 'b', type: 'quantitative'}}}), null, 2}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT038", "General Mermaid", "SECTION_MERMAID", "BLOCK_MERMAID", "BLOCK_MERMAID",
				"{widget: 'mermaid', data: {text: GENERAL_MERMAID}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT039", "Class Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_CLASS_DIAGRAM", "BLOCK_MERMAID_CLASS_DIAGRAM",
				"{widget: 'mermaid', data: {text: CLASS_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT040", "Sequence Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_SEQUENCE_DIAGRAM", "BLOCK_MERMAID_SEQUENCE_DIAGRAM",
				"{widget: 'mermaid', data: {text: SEQUENCE_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT041", "State Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_STATE_DIAGRAM", "BLOCK_MERMAID_STATE_DIAGRAM",
				"{widget: 'mermaid', data: {text: STATE_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT042", "Entity Relationship Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_RELATIONSHIP_ENTITY", "BLOCK_MERMAID_RELATIONSHIP_ENTITY",
				"{widget: 'mermaid', data: {text: ENTITY_RELATIONSHIP_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT043", "User Journey", "SECTION_MERMAID", "BLOCK_MERMAID_JOURNEY", "BLOCK_MERMAID_JOURNEY",
			    "{widget: 'mermaid', data: {text: USER_JOURNEY}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT044", "Gantt", "SECTION_MERMAID", "BLOCK_MERMAID_GANTT", "BLOCK_MERMAID_GANTT",
			    "{widget: 'mermaid', data: {text: GANTT}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT045", "Pie Chart", "SECTION_MERMAID", "BLOCK_MERMAID_PIECHART", "BLOCK_MERMAID_PIECHART",
			    "{widget: 'mermaid', data: {text: PIE_CHART}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT046", "Quadrant Chart", "SECTION_MERMAID", "BLOCK_MERMAID_QUADRANT_CHART", "BLOCK_MERMAID_QUADRANT_CHART",
		        "{widget: 'mermaid', data: {text: QUADRANT_CHART}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT047", "Requirement Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_REQUIREMENT_DIAGRAM", "BLOCK_MERMAID_REQUIREMENT_DIAGRAM",
		        "{widget: 'mermaid', data: {text: REQUIREMENT_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT048", "Git Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_GIT_DIAGRAM", "BLOCK_MERMAID_GIT_DIAGRAM",
		        "{widget: 'mermaid', data: {text: GIT_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT049", "C4 Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_C4_DIAGRAM", "BLOCK_MERMAID_C4_DIAGRAM",
		        "{widget: 'mermaid', data: {text: C4_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT050", "Mindmap", "SECTION_MERMAID", "BLOCK_MERMAID_MINDMAP", "BLOCK_MERMAID_MINDMAP",
		        "{widget: 'mermaid', data: {text: MINDMAP}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT051", "Timeline", "SECTION_MERMAID", "BLOCK_MERMAID_TIMELINE", "BLOCK_MERMAID_TIMELINE",
		        "{widget: 'mermaid', data: {text: TIMELINE}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT052", "Sankey", "SECTION_MERMAID", "BLOCK_MERMAID_SANKEY", "BLOCK_MERMAID_SANKEY",
		        "{widget: 'mermaid', data: {text: SANKEY}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT053", "XY Chart", "SECTION_MERMAID", "BLOCK_MERMAID_XY_CHART", "BLOCK_MERMAID_XY_CHART",
		        "{widget: 'mermaid', data: {text: XY_Chart}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT054", "Block Diagram", "SECTION_MERMAID", "BLOCK_MERMAID_DIAGRAM", "BLOCK_MERMAID_DIAGRAM",
		        "{widget: 'mermaid', data: {text: BLOCK_DIAGRAM}, listeners: {}, slots: {}}", "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);

		}
	
	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * @return
	 */
	public static boolean isInitalized() {
		return AbstractThemeUtils.initialized;
	}
	
	static List<Map<String, Object>> flushRsToMap(IRawSelectWrapper wrapper) {
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			Map<String, Object> map = new HashMap<String, Object>();
			for(int i = 0; i < headers.length; i++) {
				if(values[i] instanceof java.sql.Clob) {
					try {
						map.put(headers[i], IOUtils.toString(((java.sql.Clob) values[i]).getAsciiStream()));
					} catch (IOException | SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error occurred trying to read theme map");
					}
				} else {
					map.put(headers[i], values[i]);
				}
			}
			result.add(map);
		}
		return result;
	}
	
	static Object flushRsToObject(IRawSelectWrapper wrapper) {
		Object obj = null;
		if(wrapper.hasNext()) {
			obj = wrapper.next().getValues()[0];
			if(obj instanceof java.sql.Clob) {
				try {
					obj = IOUtils.toString(((java.sql.Clob) obj).getAsciiStream());
				} catch (IOException | SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return obj;
	}
}
