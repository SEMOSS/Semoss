package prerna.theme;

import java.lang.reflect.Type;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.MalformedJsonException;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class BlocksThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(BlocksThemeUtils.class);

	private static BlocksThemeUtils instance = new BlocksThemeUtils();

	public static final ArrayList<String> BASE_BLOCKS = new ArrayList<String>(Arrays.asList("Audio Player", "Button",
			"Checkbox", "Input", "Select", "Upload", "Container", "Progress", "Iframe", "PDF Viewer", "Image", "Logs",
			"Toggle Button", "Link", "Markdown", "HTML", "Text H1", "Text H2", "Text H3", "Text H4",
			"Text H5", "Text H6", "Text P", "Text P Italics", "Compare LLMs", "Mermaid", "Vega", "Grid", "Bar Chart",
			"Grouped Bar Chart", "Pie Chart", "Radial Plot", "Line Chart", "Area Chart", "Area Chart with Gradient",
			"Scatter Plot", "General Mermaid", "Class Diagram", "Sequence Diagram", "State Diagram",
			"Entity Relationship Diagram", "User Journey", "Gantt", "Pie Chart", "Quadrant Chart",
			"Requirement Diagram", "Git Diagram", "C4 Diagram", "Mindmap", "Timeline", "Sankey", "XY Chart",
			"Block Diagram", "Theme Block", "Page Block"));

	private static final String BLOCK_QUERY = "INSERT INTO BLOCKS_TEMPLATE (ID, NAME, SECTION, IMAGE, HOVER_IMAGE, HOVER_TEXT, BLOCK_JSON, CLASSIFICATION, IS_DELETABLE, DATE_ADDED, IS_LATEST) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	public static String[] BLOCK_COLUMN_NAMES = new String[] { "ID", "NAME", "SECTION", "IMAGE", "HOVER_IMAGE", "HOVER_TEXT", "BLOCK_JSON" , "CLASSIFICATION", "IS_DELETABLE", "DATE_ADDED", "IS_LATEST"};

	public static final List<Object[]> BLOCKS_DEFAULT_ENTRIES = new ArrayList<Object[]>(Arrays.asList(
			new Object[] {
					"BT001", "Audio Player", "Input", "AUDIO_PLAYER_ACTIVE", "AUDIO_PLAYER_HOVER", "Play back audio responses or other files", 
					new JSONObject("{widget: 'audio-player', data: {label: 'Audio Player', autoplay: false, controls: true, loop: false, source: ''}, listeners: {}, slots: {}}"), "DEFAULT", false, Utility.getCurrentSqlTimestampUTC(), true
				},
			new Object[] {
			        "BT002", "Button", "Input", "BUTTON_ACTIVE", "BUTTON_HOVER", "Creates a click event", 
			new JSONObject("{widget: 'button', data: {style: {}, label: 'Submit', loading: false, disabled: false, variant: 'contained', color: 'primary'}, listeners: {onClick: []}, slots: {}}"), "DEFAULT", 
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT003", "Checkbox", "Input", "CHECKBOX_ACTIVE", "CHECKBOX_HOVER", "Add a checkbox for user  selection", 
			new JSONObject("{widget: 'checkbox', data: {style: {padding: 'none'}, label: 'Example Checkbox', required: false, disabled: false, value: false}, listeners: {onChange: []}, slots: {}}"), "DEFAULT", 
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT004", "Input", "Input", "AUDO_INPUT_ACTIVE", "AUDO_INPUT_HOVER", "Add an input box for typing  text", 
			new JSONObject("{widget: 'input', data: {style: {width: '100%', padding: '4px'}, value: '', label: 'Example Input', hint: '', type: 'text', rows: 1, multiline: false, disabled: false, required: false, loading: false}, listeners: {onChange: []}, slots: {content: []}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT005", "Select", "Input", "SELECT_ACTIVE", "SELECT_HOVER", "Choose an option from a dropdown list", 
			new JSONObject("{widget: 'select', data: {style: {padding: '4px'}, value: '', label: 'Example Select Input', hint: '', options: [], required: false, disabled: false, loading: false}, listeners: {onChange: []}, slots: {content: []}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT006", "Upload", "Input", "UPLOAD_ACTIVE", "UPLOAD_HOVER", "Upload files like documents or images", 
			new JSONObject("{widget: 'upload', data: {style: {width: '100%', padding: '4px'}, value: '', label: 'Example Input', hint: '', loading: false, disabled: false, required: false}, listeners: {onChange: []}, slots: {content: []}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT007", "Container", "Layout", "CONTAINER_ACTIVE", "CONTAINER_HOVER", "Create a layout element for custom design", 
			new JSONObject("{widget: 'container', data: {style: {display: 'flex', flexDirection: 'column', padding: '4px', gap: '8px', flexWrap: 'wrap'}}, listeners: {}, slots: {children: []}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT008", "Progress", "Progress", "PROGRESS_ACTIVE", "PROGRESS_HOVER", "Display progress tracking or status", 
			new JSONObject("{widget: 'progress', data: {type: 'linear', value: 50, includeLabel: true, size: '300px'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT009", "Iframe", "Element", "IFRAME_ACTIVE", "IFRAME_HOVER", "Embed a webpage using a source link", 
			new JSONObject("{widget: 'iframe', data: {style: {}, src: '', title: '', enableFrameInteractions: true}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT010", "PDF Viewer", "Element", "PDF_ACTIVE", "PDF_HOVER", "Embed a PDF for viewing", 
			new JSONObject("{widget: 'pdfViewer', data: {style: {width: '100%', height: '82%', padding: '8px'}, selectedPdf: null}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT011", "Image", "Element", "IMAGE_ACTIVE", "IMAGE_HOVER", "Add an image to your layout", 
			new JSONObject("{widget: 'image', data: {style: {display: 'flex', justifyContent: 'center', alignItems: 'center', width: '100%', height: '200px', backgroundSize: 'contain', backgroundRepeat: 'no-repeat', backgroundPosition: 'center center'}, src: '', title: ''}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT012", "Logs", "Text", "LOGS_ACTIVE", "LOGS_HOVER", "Display logs for tracking  events or data", 
			        new JSONObject("{widget: 'logs', data: {style: {}, queryId: ''}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT013", "Toggle Button", "Input", "TOGGLE_ACTIVE", "TOGGLE_HOVER", "Switch between multiple options", 
			        new JSONObject("{widget: 'toggle-button', data: {disabled: false, color: 'primary', size: 'small', options: [{display: 'on', value: 'on'}, {display: 'off', value: 'off'}], value: null, mandatory: true, multiple: false}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT014", "Link", "Text", "LINK_ACTIVE", "LINK_HOVER", "Access a webpage through a clickable URL", 
			        new JSONObject("{widget: 'link', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, href: '', text: 'Insert text'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT015", "Markdown", "Text", "MARKDOWN_ACTIVE", "MARKDOWN_HOVER", "Show text in markdown format", 
			        new JSONObject("{widget: 'markdown', data: {style: {padding: '4px'}, markdown: '**Hello world**'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT016", "HTML", "Element", "HTML_ACTIVE", "HTML_HOVER", "Write custom HTML manually or with AI assistance", 
			        new JSONObject("{widget: 'html', data: {style: {padding: '4px'}, html: '<html>\\r\\n <style>\\r\\n html {\\r\\n font-family: Roboto;\\r\\n text-align: center;\\r\\n overflow: hidden;\\r\\n}\\r\\n </style>\\r\\n <body>\\r\\n <h2>HTML Block</h2>\\r\\n </body>\\r\\n</html>'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT018", "Text H1", "Text", "H1_ACTIVE", "H1_HOVER", "Display text in header 1", 
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h1'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT019", "Text H2", "Text", "H2_ACTIVE", "H2_HOVER", "Display text in header 2",
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h2'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT020", "Text H3", "Text", "H3_ACTIVE", "H3_HOVER", "Display text in header 3",
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h3'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT021", "Text H4", "Text", "H4_ACTIVE", "H4_HOVER", "Display text in header 4", 
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h4'}, listeners: {}, slots: {}}"), "DEFAULT",
		        false, Utility.getCurrentSqlTimestampUTC(), true
		    },
			new Object[] {
			        "BT022", "Text H5", "Text", "H5_ACTIVE", "H5_HOVER","Display text in header 5", 
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h5'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT023", "Text H6", "Text", "H6_ACTIVE", "H6_HOVER", "Display text in header 6", 
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'h6'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT024", "Text P", "Text", "PARAGRAPH_ACTIVE", "PARAGRAPH_HOVER", "Display text in a regular paragraph style", 
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Hello world', variant: 'p'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT025", "Text P Italics", "Text", null, null,  null,
			        new JSONObject("{widget: 'text', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis', fontStyle: 'italic'}, text: 'Hello world', variant: 'p'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT026", "Compare LLMs", "Compare LLMs", "COMPARE_LLM_ACTIVE", "COMPARE_LLM_HOVER", "Compare large language models against the same context", 
			        new JSONObject("{widget: 'llmComparison', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: '', variants: {}}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT027", "Mermaid", "Mermaid", "MERMAIDJS_ACTIVE", "MERMAIDJS_HOVER",  null,
			        new JSONObject("{widget: 'mermaid', data: {style: {padding: '4px', whiteSpace: 'pre-line', textOverflow: 'ellipsis'}, text: 'Query', variant: 'p'}, listeners: {}, slots: {}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT028", "Vega", "General Visualization", null, null, null,
			        new JSONObject("{widget: 'vega', data: {specJson: '', variation: undefined}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT029", "Grid", "General Visualization", null, null, null,
			        new JSONObject("{widget: 'grid', data: {frame: {name: ''}, columns: [], view: {pagination: true}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT030", "Bar Chart", "Bar Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'bar-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Bar Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'C', b: 43}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'F', b: 53}, {a: 'G', b: 19}, {a: 'H', b: 87}, {a: 'I', b: 52}]}, mark: 'bar', encoding: {x: {field: 'a', type: 'ordinal'}, y: {field: 'b', type: 'quantitative'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT031", "Grouped Bar Chart", "Bar Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'grouped-bar-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Grouped Bar Chart', width: 300, height: 300, data: {values: [{category: 'A', group: 'x', value: 0.1}, {category: 'A', group: 'y', value: 0.6}, {category: 'A', group: 'z', value: 0.9}, {category: 'B', group: 'x', value: 0.7}, {category: 'B', group: 'y', value: 0.2}, {category: 'B', group: 'z', value: 1.1}, {category: 'C', group: 'x', value: 0.6}, {category: 'C', group: 'y', value: 0.1}, {category: 'C', group: 'z', value: 0.2}]}, mark: 'bar', encoding: {x: {field: 'category'}, y: {field: 'value', type: 'quantitative'}, xOffset: {field: 'group'}, color: {field: 'group'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT032", "Pie Chart", "Pie Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'pie-chart', specJson: \"{$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Pie Chart', width: 300, height: 300, description: 'A simple pie chart with embedded data.', data: {values: [{category: 1, value: 4}, {category: 2, value: 6}, {category: 3, value: 10}, {category: 4, value: 3}, {category: 5, value: 7}, {category: 6, value: 8}]}, mark: 'arc', encoding: {theta: {field: 'value', type: 'quantitative'}, color: {field: 'category', type: 'nominal'}}}\"}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT033", "Radial Plot", "Pie Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'radial-plot', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Radial Plot', width: 300, height: 300, description: 'A simple radial chart with embedded data.', data: {values: [12, 23, 47, 6, 52, 19]}, layer: [{mark: {type: 'arc', innerRadius: 20, stroke: '#fff'}}, {mark: {type: 'text', radiusOffset: 10}, encoding: {text: {field: 'data', type: 'quantitative'}}}], encoding: {theta: {field: 'data', type: 'quantitative', stack: true}, radius: {field: 'data', scale: {type: 'sqrt', zero: true, rangeMin: 20}}, color: {field: 'data', type: 'nominal', legend: null}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT034", "Line Chart", "Line Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'line-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Line Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55, predicted: false}, {a: 'D', b: 91, predicted: false}, {a: 'E', b: 81, predicted: false}, {a: 'E', b: 81, predicted: true}, {a: 'G', b: 19, predicted: true}, {a: 'H', b: 87, predicted: true}]}, mark: 'line', encoding: {x: {field: 'a', type: 'ordinal'}, y: {field: 'b', type: 'quantitative'}, strokeDash: {field: 'predicted', type: 'nominal'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT035", "Area Chart", "Area Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'area-chart', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Area Chart', width: 300, height: 300, data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'E', b: 81}, {a: 'G', b: 19}, {a: 'H', b: 87}]}, mark: 'area', encoding: {x: {field: 'a'}, y: {aggregate: 'sum', field: 'b', title: 'count'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
		        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT036", "Area Chart with Gradient", "Area Chart", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'area-chart-with-gradient', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Area Chart with Gradient', width: 300, height: 300, description: 'Simple area chart with gradient.', data: {values: [{a: 'A', b: 28}, {a: 'B', b: 55}, {a: 'D', b: 91}, {a: 'E', b: 81}, {a: 'E', b: 81}, {a: 'G', b: 19}, {a: 'H', b: 87}]}, mark: {type: 'area', line: {color: 'darkgreen'}, color: {x1: 1, y1: 1, x2: 1, y2: 0, gradient: 'linear', stops: [{offset: 0, color: 'white'}, {offset: 1, color: 'darkgreen'}]}}, encoding: {x: {field: 'a'}, y: {aggregate: 'sum', field: 'b', title: 'count'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT037", "Scatter Plot", "Scatter Plots", null, null, null,
			        new JSONObject("{widget: 'vega', data: {variation: 'scatter-plot', specJson: {$schema: 'https://vega.github.io/schema/vega-lite/v5.json', title: 'Scatter Plot', width: 300, height: 300, description: 'A scatterplot.', data: {values: [{a: 10, b: 28}, {a: 20, b: 55}, {a: 30, b: 91}, {a: 40, b: 81}, {a: 50, b: 81}, {a: 60, b: 19}, {a: 70, b: 87}]}, mark: 'point', encoding: {x: {field: 'a', type: 'quantitative'}, y: {field: 'b', type: 'quantitative'}}}}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT038", "General Mermaid", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: GENERAL_MERMAID}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT039", "Class Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: CLASS_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT040", "Sequence Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: SEQUENCE_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT041", "State Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: STATE_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT042", "Entity Relationship Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: ENTITY_RELATIONSHIP_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT043", "User Journey", "Mermaid", null, null, null,
			        new JSONObject( "{widget: 'mermaid', data: {text: USER_JOURNEY}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT044", "Gantt", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: GANTT}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT045", "Pie Chart", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: PIE_CHART}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT046", "Quadrant Chart", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: QUADRANT_CHART}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT047", "Requirement Diagram", "Mermaid",  null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: REQUIREMENT_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT048", "Git Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: GIT_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT049", "C4 Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: C4_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT050", "Mindmap", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: MINDMAP}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT051", "Timeline", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: TIMELINE}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT052", "Sankey", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: SANKEY}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT053", "XY Chart", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: XY_Chart}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT054", "Block Diagram", "Mermaid", null, null, null,
			        new JSONObject("{widget: 'mermaid', data: {text: BLOCK_DIAGRAM}, listeners: {}, slots: {}}"), "VISUALIZATION",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT055", "Theme Block", "Theme", null, null, null,
			        new JSONObject("{widget: 'theme', data: {theme: lightTheme}, listeners: {}, slots: {children: []}}"), "DEFAULT",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    },
			new Object[] {
			        "BT056", "Page Block", "Page", null, null, null,
			        new JSONObject("{widget: 'page', data: {style: {display: 'flex', flexDirection: 'column', padding: '24px', gap: '8px', fontFamily: 'roboto'}, route: ''}, listeners: {onPageLoad: []}, slots: {content: []}}"), "LAYER",
			        false, Utility.getCurrentSqlTimestampUTC(), true
			    }
			));
	
	private BlocksThemeUtils() {

	}

	
	private static ThemeDbTable validateThemeDbTable(String tablename) {
		ThemeDbTable table = ThemeDbTable.valueOf(tablename);
		if (table == null || !table.equals(ThemeDbTable.BLOCKS_TEMPLATE)) {
			throw new IllegalArgumentException("Requested table not found");
		}
		return table;
	}

	
	public static List<String> getBlockNames() throws SQLException {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "IS_DELETABLE", "==", false,
				PixelDataType.BOOLEAN));

		List<Map<String, Object>> queryRes = null;
		try {
			queryRes = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (queryRes == null || queryRes.isEmpty()) {
			return new ArrayList<>();
		}

		List<String> output = queryRes.parallelStream().map(mapObj -> (String) mapObj.get("NAME"))
				.collect(Collectors.toList());

		return output;
	}
	
	public static List<Map<String, Object>> getThemeData(String tableName, GenRowFilters filters) throws SQLException {
		ThemeDbTable table = validateThemeDbTable(tableName);

		final String blocksPrefix = table.getThemeDbTablePrefix();

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : BlocksThemeUtils.BLOCK_COLUMN_NAMES) {
			qs.addSelector(new QueryColumnSelector(blocksPrefix + colName));
		}
		
		if(filters != null) {
			qs.mergeExplicitFilters(filters);
		}
		
		List<Map<String, Object>> retVal = null;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new ArrayList<>();
		}
		
		List<Map<String, Object>> actualRetVal = retVal.stream()
			    .map((map) -> {
			        convertBlockJsonStringToJSONObject(map);
			        return map;
			    })
			    .collect(Collectors.toList());


		return actualRetVal;
	}
	
	private static void convertBlockJsonStringToJSONObject(Map<String, Object> map) {
		try {
			String blockJson = (String) map.get("BLOCK_JSON");
			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>(){}.getType();
			map.put("BLOCK_JSON", gson.fromJson(blockJson, type));
		} catch (Exception e) {
			throw new SemossPixelException(e);
		}
	}

	
	public static Map<String, Object> getBlock(String blockId, String tableName) throws SQLException {
		ThemeDbTable table = validateThemeDbTable(tableName);

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : BlocksThemeUtils.BLOCK_COLUMN_NAMES) {
			qs.addSelector(new QueryColumnSelector(table.getThemeDbTablePrefix() + colName));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "ID", "==",
						blockId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "IS_LATEST", "==", 1));

		List<Map<String, Object>> retVal = null;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new HashMap<>();
		}

		return retVal.get(0);
	}
	
	public static boolean deleteBlock(String blockId, String tableName, boolean hardDelete) throws SQLException {
		ThemeDbTable table = validateThemeDbTable(tableName);

		try {
			if (!isDeletable(blockId, table)) {
				throw new SecurityException("Not allowed to delete this block");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}

		if (hardDelete) {
			String query = "DELETE FROM " + table.getThemeDbTableName() + " WHERE ID = ?";
			PreparedStatement ps = null;

			try {
				ps = themeDb.getPreparedStatement(query);
				ps.setString(1, blockId);
				int rowsAffected = ps.executeUpdate();
				return (rowsAffected > 0);
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				return false;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
			}
		} else {
			return updateBlock(blockId);
		}
	}
	
	
	private static boolean isDeletable(String blockId, ThemeDbTable table) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table.getThemeDbTablePrefix() + "IS_DELETABLE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "ID", "==",
						blockId, PixelDataType.CONST_STRING));
		List<Map<String, Object>> retVal = null;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			throw new IllegalArgumentException("Block Id does not exist");
		}

		return (boolean) retVal.get(0).get("IS_DELETABLE");
	}

	
	// add block function
	public static String addBlock(Map<String, Object> blockDetails, String tableName) {
		ThemeDbTable table = validateThemeDbTable(tableName);
		
		boolean allowClob = themeDb.getQueryUtil().allowClobJavaObject();
		String blockId = UUID.randomUUID().toString();
		blockDetails.put("id", blockId);
		validateBlockDetails(blockDetails);
		insertBlock(blockDetails, allowClob, blockId);
		return blockId;
	}

	
	// edit block function 
	public static boolean editBlock(Map<String, Object> editDetails, String tableName) {
		ThemeDbTable table = validateThemeDbTable(tableName);

		boolean allowClob = themeDb.getQueryUtil().allowClobJavaObject();
		String blockId = (String) editDetails.get("id");
		try {
			if (!isDeletable(blockId, table)) {
				throw new SecurityException("Not allowed to delete this block");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
		updateBlock(blockId);
		insertBlock(editDetails, allowClob, blockId);
		return true;
	}

	
	// validate the input map for required fields
	private static void validateBlockDetails(Map<String, Object> blockDetails) {
		validateString(blockDetails, "id", false, false);
		validateString(blockDetails, "name", false, false);
		validateString(blockDetails, "section", false, false);
		validateString(blockDetails, "image", false, false);
		validateString(blockDetails, "block_json", false, false);
	}

	
	// validate the individual fields
	private static void validateString(Map<String, Object> blockDetails, String mapKey, boolean nullable,
			boolean allowEmpty) {
		String value = null;
		try {
			value = (String) blockDetails.get(mapKey);
			value = value != null ? value.trim() : value;
			if (value == null && !nullable) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Block");
			}
			if (value != null && value.isEmpty() && !allowEmpty) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Block");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	
	// insert the row into blocks_template table
	private static void insertBlock(Map<String, Object> blockDetails, boolean allowClob, String blockId) {
		PreparedStatement blockPS = null;
		try {
			blockPS = themeDb.getPreparedStatement(BLOCK_QUERY);
			int parameterIndex = 1;
			blockPS.setString(parameterIndex++, blockId);
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("name")));
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("section")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("image")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("image")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("hover_text")));
			if (allowClob) {
				Clob toclob = themeDb.getConnection().createClob();
				toclob.setString(1, String.valueOf(blockDetails.get("block_json")));
				blockPS.setClob(parameterIndex++, toclob);
			} else {
				blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("block_json")));
			}
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("classification")).toUpperCase());
			blockPS.setBoolean(parameterIndex++, true);
			blockPS.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			blockPS.setBoolean(parameterIndex++, true);
			blockPS.executeUpdate();
			if (!blockPS.getConnection().getAutoCommit()) {
				blockPS.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, null, blockPS, null);
		}
	}

	
	// update the row in blocks_template associated with the ID to be latest
	// (similar to soft delete)
	private static boolean updateBlock(String blockId) {
		String[] colToUpdate = { "IS_LATEST" };
		String[] whereCol = { "ID" };
		String promptPermissionQuery = themeDb.getQueryUtil().createUpdatePreparedStatementString("BLOCKS_TEMPLATE",
				colToUpdate, whereCol);
		PreparedStatement ps = null;
		try {
			ps = themeDb.getPreparedStatement(promptPermissionQuery);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, false);
			ps.setString(parameterIndex++, blockId);
			int rowsAffected = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return (rowsAffected > 0);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}

	}

	public static String[] getThemeColTypes(AbstractSqlQueryUtil queryUtil) {
		return new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(500)", queryUtil.getClobDataTypeName(), "varchar(255)", queryUtil.getBooleanDataTypeName(), queryUtil.getDateWithTimeDataType(), queryUtil.getBooleanDataTypeName() };
	}

}
