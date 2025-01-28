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
import org.json.JSONObject;

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
	
	public static String[] blocksTemplateColNames = new String[] { "ID", "NAME", "SECTION", "IMAGE", "HOVER_IMAGE", "HOVER_TEXT", "BLOCK_JSON" , "CLASSIFICATION", "IS_DELETABLE", "DATE_ADDED", "IS_LATEST"};
	
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
		
		blocksTemplateTypes = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(500)", queryUtil.getClobDataTypeName(), "varchar(255)", queryUtil.getBooleanDataTypeName(), queryUtil.getDateWithTimeDataType(), queryUtil.getBooleanDataTypeName() };
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
						"BT001", "Audio Player", "Input", "AUDIO_PLAYER_ACTIVE", "AUDIO_PLAYER_HOVER", "Play back audio responses or other files", 
						new JSONObject("{widget: 'audio-player', data: {label: 'Audio Player', autoplay: false, controls: true, loop: false, source: ''}, listeners: {}, slots: {}}"), "DEFAULT", false, Utility.getCurrentSqlTimestampUTC(), true
					})
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT002", "Button", "Input", "BUTTON_ACTIVE", "BUTTON_HOVER", "Creates a click event", 
				new JSONObject("{widget: 'button', data: {style: {}, label: 'Submit', loading: false, disabled: false, variant: 'contained', color: 'primary'}, listeners: {onClick: []}, slots: {}}"), "DEFAULT", 
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT003", "Checkbox", "Input", "CHECKBOX_ACTIVE", "CHECKBOX_HOVER", "Add a checkbox for user  selection", 
				new JSONObject("{widget: 'checkbox', data: {style: {padding: 'none'}, label: 'Example Checkbox', required: false, disabled: false, value: false}, listeners: {onChange: []}, slots: {}}"), "DEFAULT", 
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT004", "Input", "Input", "AUDO_INPUT_ACTIVE", "AUDO_INPUT_HOVER", "Add an input box for typing  text", 
				new JSONObject("{widget: 'input', data: {style: {width: '100%', padding: '4px'}, value: '', label: 'Example Input', hint: '', type: 'text', rows: 1, multiline: false, disabled: false, required: false, loading: false}, listeners: {onChange: []}, slots: {content: []}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT005", "Select", "Input", "SELECT_ACTIVE", "SELECT_HOVER", "Choose an option from a dropdown list", 
				new JSONObject("{widget: 'select', data: {style: {padding: '4px'}, value: '', label: 'Example Select Input', hint: '', options: [], required: false, disabled: false, loading: false}, listeners: {onChange: []}, slots: {content: []}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT006", "Upload", "Input", "UPLOAD_ACTIVE", "UPLOAD_HOVER", "Upload files like documents or images", 
				new JSONObject("{widget: 'upload', data: {style: {width: '100%', padding: '4px'}, value: '', label: 'Example Input', hint: '', loading: false, disabled: false, required: false}, listeners: {onChange: []}, slots: {content: []}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT007", "Container", "Layout", "CONTAINER_ACTIVE", "CONTAINER_HOVER", "Create a layout element for custom design", 
				new JSONObject("{widget: 'container', data: {style: {display: 'flex', flexDirection: 'column', padding: '4px', gap: '8px', flexWrap: 'wrap'}}, listeners: {}, slots: {children: []}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT008", "Progress", "Progress", "PROGRESS_ACTIVE", "PROGRESS_HOVER", "Display progress tracking or status", 
				new JSONObject("{widget: 'progress', data: {type: 'linear', value: 50, includeLabel: true, size: '300px'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT009", "Iframe", "Element", "IFRAME_ACTIVE", "IFRAME_HOVER", "Embed a webpage using a source link", 
				new JSONObject("{widget: 'iframe', data: {style: {}, src: '', title: '', enableFrameInteractions: true}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT010", "PDF Viewer", "Element", "PDF_ACTIVE", "PDF_HOVER", "Embed a PDF for viewing", 
				new JSONObject("{widget: 'pdfViewer', data: {style: {width: '100%', height: '82%', padding: '8px'}, selectedPdf: null}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT011", "Image", "Element", "IMAGE_ACTIVE", "IMAGE_HOVER", "Add an image to your layout", 
				new JSONObject("{widget: 'image', data: {style: {display: 'flex', justifyContent: 'center', alignItems: 'center', width: '100%', height: '200px', backgroundSize: 'contain', backgroundRepeat: 'no-repeat', backgroundPosition: 'center center'}, src: '', title: ''}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT012", "Logs", "Text", "LOGS_ACTIVE", "LOGS_HOVER", "Display logs for tracking  events or data", 
				        new JSONObject("{widget: 'logs', data: {style: {}, queryId: ''}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT013", "Toggle Button", "Input", "TOGGLE_ACTIVE", "TOGGLE_HOVER", "Switch between multiple options", 
				        new JSONObject("{widget: 'toggle-button', data: {disabled: false, color: 'primary', size: 'small', options: [{display: 'on', value: 'on'}, {display: 'off', value: 'off'}], value: null, mandatory: true, multiple: false}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT014", "Link", "Text", "LINK_ACTIVE", "LINK_HOVER", "Access a webpage through a clickable URL", 
				        new JSONObject("{widget: 'link', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, href: '', text: 'Insert text'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT015", "Markdown", "Text", "MARKDOWN_ACTIVE", "MARKDOWN_HOVER", "Show text in markdown format", 
				        new JSONObject("{widget: 'markdown', data: {style: {padding: '4px'}, markdown: '**Hello world**'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT016", "HTML", "Element", "HTML_ACTIVE", "HTML_HOVER", "Write custom HTML manually or with AI assistance", 
				        new JSONObject("{widget: 'html', data: {style: {padding: '4px'}, html: '<html>\\r\\n <style>\\r\\n html {\\r\\n font-family: Roboto;\\r\\n text-align: center;\\r\\n overflow: hidden;\\r\\n}\\r\\n </style>\\r\\n <body>\\r\\n <h2>HTML Block</h2>\\r\\n </body>\\r\\n</html>'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT018", "Text H1", "Text", "H1_ACTIVE", "H1_HOVER", "Display text in header 1", 
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h1'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT019", "Text H2", "Text", "H2_ACTIVE", "H2_HOVER", "Display text in header 2",
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h2'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT020", "Text H3", "Text", "H3_ACTIVE", "H3_HOVER", "Display text in header 3",
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h3'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
					    new Object[] {
					        "BT021", "Text H4", "Text", "H4_ACTIVE", "H4_HOVER", "Display text in header 4", 
					        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h4'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT022", "Text H5", "Text", "H5_ACTIVE", "H5_HOVER","Display text in header 5", 
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h5'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT023", "Text H6", "Text", "H6_ACTIVE", "H6_HOVER", "Display text in header 6", 
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h6'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT024", "Text P", "Text", "PARAGRAPH_ACTIVE", "PARAGRAPH_HOVER", "Display text in a regular paragraph style", 
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'p'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT025", "Text P Italics", "Text", null, null,  null,
				        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis', fontStyle: 'italic'}, text: 'Hello world', variant: 'p'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT026", "Compare LLMs", "Compare LLMs", "COMPARE_LLM_ACTIVE", "COMPARE_LLM_HOVER", "Compare large language models against the same context", 
				        new JSONObject("{widget: 'llmComparison', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: '', variants: {}}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] { // TODO: change hover image once updated
				        "BT027", "Mermaid", "Mermaid", "MERMAIDJS_ACTIVE", null,  null,
				        new JSONObject("{widget: 'mermaid', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Query', variant: 'p'}, listeners: {}, slots: {}}"), "DEFAULT",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				// Insert visualization blocks
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT028", "Vega", "General Visualization", null, null, null,
				        new JSONObject("{widget: 'vega', data: {specJson: '', variation: undefined}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT029", "Grid", "General Visualization", null, null, null,
				        new JSONObject("{widget: 'grid', data: {frame: {name: ''}, columns: [], view: {pagination: true}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT030", "Bar Chart", "Bar Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'bar-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Bar Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'C', b: 43}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'F', b: 53}, {a: 'G', b: 19}, {a: 'H', b: 87}, {a: 'I', b: 52}]}, mark: 'bar', encoding: {x: {field: 'a', type: 'ordinal'}, y: {field: 'b', type: 'quantitative'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT031", "Grouped Bar Chart", "Bar Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'grouped-bar-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Grouped Bar Chart', width: 300, height: 300, data: {values: [{category: 'A', group: 'x', value: 0.1}, {category: 'A', group: 'y', value: 0.6}, {category: 'A', group: 'z', value: 0.9}, {category: 'B', group: 'x', value: 0.7}, {category: 'B', group: 'y', value: 0.2}, {category: 'B', group: 'z', value: 1.1}, {category: 'C', group: 'x', value: 0.6}, {category: 'C', group: 'y', value: 0.1}, {category: 'C', group: 'z', value: 0.2}]}, mark: 'bar', encoding: {x: {field: 'category'}, y: {field: 'value', type: 'quantitative'}, xOffset: {field: 'group'}, color: {field: 'group'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT032", "Pie Chart", "Pie Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'pie-chart', specJson: \"{$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Pie Chart', width: 300, height: 300, description: 'A simple pie chart with embedded data.', data: {values: [{category: 1, value: 4}, {category: 2, value: 6}, {category: 3, value: 10}, {category: 4, value: 3}, {category: 5, value: 7}, {category: 6, value: 8}]}, mark: 'arc', encoding: {theta: {field: 'value', type: 'quantitative'}, color: {field: 'category', type: 'nominal'}}}\"}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT033", "Radial Plot", "Pie Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'radial-plot', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Radial Plot', width: 300, height: 300, description: 'A simple radial chart with embedded data.', data: {values: [12, 23, 47, 6, 52, 19]}, layer: [{mark: {type: 'arc', innerRadius: 20, stroke: '#fff'}}, {mark: {type: 'text', radiusOffset: 10}, encoding: {text: {field: 'data', type: 'quantitative'}}}], encoding: {theta: {field: 'data', type: 'quantitative', stack: true}, radius: {field: 'data', scale: {type: 'sqrt', zero: true, rangeMin: 20}}, color: {field: 'data', type: 'nominal', legend: null}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT034", "Line Chart", "Line Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'line-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Line Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55, predicted: false}, {a: 'D', b: 91, predicted: false}, {a: 'E', b: 81, predicted: false}, {a: 'E', b: 81, predicted: true}, {a: 'G', b: 19, predicted: true}, {a: 'H', b: 87, predicted: true}]}, mark: 'line', encoding: {x: {field: 'a', type: 'ordinal'}, y: {field: 'b', type: 'quantitative'}, strokeDash: {field: 'predicted', type: 'nominal'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT035", "Area Chart", "Area Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'area-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Area Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'E', b: 81}, {a: 'G', b: 19}, {a: 'H', b: 87}]}, mark: 'area', encoding: {x: {field: 'a'}, y: {aggregate: 'sum', field: 'b', title: 'count'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT036", "Area Chart with Gradient", "Area Chart", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'area-chart-with-gradient', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Area Chart with Gradient', width: 300, height: 300, description: 'Simple area chart with gradient.', data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'E', b: 81}, {a: 'G', b: 19}, {a: 'H', b: 87}]}, mark: {type: 'area', line: {color: 'darkgreen'}, color: {x1: 1, y1: 1, x2: 1, y2: 0, gradient: 'linear', stops: [{offset: 0, color: 'white'}, {offset: 1, color: 'darkgreen'}]}}, encoding: {x: {field: 'a'}, y: {aggregate: 'sum', field: 'b', title: 'count'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT037", "Scatter Plot", "Scatter Plots", null, null, null,
				        new JSONObject("{widget: 'vega', data: {variation: 'scatter-plot', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Scatter Plot', width: 300, height: 300, description: 'A scatterplot.', data: {values: [{a: 10, b: 28}, {a: 20, b: 55}, {a: 30, b: 91}, {a: 40, b: 81}, {a: 50, b: 81}, {a: 60, b: 19}, {a: 70, b: 87}]}, mark: 'point', encoding: {x: {field: 'a', type: 'quantitative'}, y: {field: 'b', type: 'quantitative'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT038", "General Mermaid", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: GENERAL_MERMAID}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT039", "Class Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: CLASS_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT040", "Sequence Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: SEQUENCE_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT041", "State Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: STATE_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT042", "Entity Relationship Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: ENTITY_RELATIONSHIP_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT043", "User Journey", "Mermaid", null, null, null,
				        new JSONObject( "{widget: 'mermaid', data: {text: USER_JOURNEY}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT044", "Gantt", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: GANTT}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT045", "Pie Chart", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: PIE_CHART}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT046", "Quadrant Chart", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: QUADRANT_CHART}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT047", "Requirement Diagram", "Mermaid",  null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: REQUIREMENT_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT048", "Git Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: GIT_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT049", "C4 Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: C4_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT050", "Mindmap", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: MINDMAP}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT051", "Timeline", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: TIMELINE}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT052", "Sankey", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: SANKEY}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT053", "XY Chart", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: XY_Chart}, listeners: {}, slots: {}}"), "VISUALIZATION",
				        false, Utility.getCurrentSqlTimestampUTC(), true
				    })
				);
				
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), blocksTemplateColNames, blocksTemplateTypes,
				    new Object[] {
				        "BT054", "Block Diagram", "Mermaid", null, null, null,
				        new JSONObject("{widget: 'mermaid', data: {text: BLOCK_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
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
