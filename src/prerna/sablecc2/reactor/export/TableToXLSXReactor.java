package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.util.CellReference;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.neo4j.io.fs.FileUtils;

import cz.vutbr.web.css.CSSException;
import cz.vutbr.web.css.CSSFactory;
import cz.vutbr.web.css.CombinedSelector;
import cz.vutbr.web.css.Declaration;
import cz.vutbr.web.css.RuleSet;
import cz.vutbr.web.css.StyleSheet;
import cz.vutbr.web.css.Term;
import cz.vutbr.web.css.TermColor;
import cz.vutbr.web.css.TermLength;
import cz.vutbr.web.css.TermPercent;
import prerna.om.Insight;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.template.TemplateUtility;
import prerna.util.Utility;

public class TableToXLSXReactor	extends AbstractReactor {

		List <Integer> autoWrappedColumns = new Vector<Integer>();
		int startColumn = 0;
		int rowGutter = 2;
		int startRow = 0;
		int rowOffset = 2;
		int columnGutter = 0;
		int lastRow = startRow;
		int lastColumn = startColumn;
		boolean keepOpen = true;
		boolean mergeCells = true; // should we merge the cells or not. 
		Map exportMap = new HashMap<String, Object>();

		int lastDataRow = 0;
		int lastDataColumn = 0;

		public static final String ROW_COUNT = "ROW_COUNT";
		public static final String COLUMN_COUNT = "COLUMN_COUNT";
		public static final String HEADER = "header";
		public static final String FOOTER = "footer";
		public static final String PLACE_HOLDER = "placeholders";
		
		Map colspanMatrix = new HashMap();
		Map rowspanMatrix = new HashMap();
		Map <Sheet, List<CellRangeAddress>> mergeAreas = new HashMap<Sheet, List <CellRangeAddress>>();
		String exportTemplate = null;
		String sheetName = null;
		String html = null;
		
		Map <Integer, Integer> columnToWidth = new HashMap<Integer, Integer>();
		
		public TableToXLSXReactor() {
			// keep open specifies whether to keep this open or close it. if kept open then this will return open as noun metadata
			// need to add table level header and tabel level footer
			
			this.keysToGet = new String[] { ReactorKeysEnum.SHEET.getKey(), ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), 
					ReactorKeysEnum.MERGE_CELLS.getKey(), ReactorKeysEnum.EXPORT_TEMPLATE.getKey(), 
					ReactorKeysEnum.HEADERS.getKey(), 
					ReactorKeysEnum.ROW_GUTTER.getKey(),
					ReactorKeysEnum.COLUMN_GUTTER.getKey(),
					ReactorKeysEnum.TABLE_HEADER.getKey(),
					ReactorKeysEnum.TABLE_FOOTER.getKey()
		
			};
			this.keyMulti = new int[] {0,0,0,0,0,1,0,0,0,0};
			this.keyRequired = new int[] {1,1,0,0,0,0,0,0,0,0};
		}

		public NounMetadata execute()
		{
			organizeKeys();
			

			String fileName = Utility.getRandomString(5);
			exportTemplate = null;

			getMap(insight.getInsightId());
			processPayload();
			 
			
			// process the table
			// set in insight so FE can download the file
			String fileLocation = processTable(sheetName, html,  fileName);
			NounMetadata retNoun = null;
			retNoun = new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
			putMap();
			// merge the cells
			if(mergeCells)
				mergeAreas();
			
			return retNoun;
		}
		
		public void processPayload()
		{
			String fileName = null;
			if(keyValue.containsKey(ReactorKeysEnum.HTML.getKey()))
				html = Utility.decodeURIComponent(keyValue.get(ReactorKeysEnum.HTML.getKey()));
			if(keyValue.containsKey(ReactorKeysEnum.SHEET.getKey()))
				sheetName = keyValue.get(ReactorKeysEnum.SHEET.getKey());
			if(keyValue.containsKey(ReactorKeysEnum.FILE_NAME.getKey()))
				fileName = keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
			else if(exportMap.containsKey("FILE_NAME"))
				fileName = (String)exportMap.get("FILE_NAME");
			
			if(fileName == null || fileName.length() == 0)
				fileName = Utility.getRandomString(5);
			
			exportMap.put("FILE_NAME", fileName);

			if(keyValue.containsKey(ReactorKeysEnum.MERGE_CELLS.getKey()))
				mergeCells = keyValue.get(ReactorKeysEnum.MERGE_CELLS.getKey()).equalsIgnoreCase("true");

			// check if any template has been selected to Export 
			if(keyValue.containsKey(ReactorKeysEnum.EXPORT_TEMPLATE.getKey()))
			{
				// fetch the template file with the provided template name and app id by calling getTemplateFile() method
				exportTemplate = TemplateUtility.getTemplateFile(keyValue.get(ReactorKeysEnum.APP.getKey()), keyValue.get(ReactorKeysEnum.EXPORT_TEMPLATE.getKey()));
			}
			exportMap.put("EXPORT_TEMPLATE", exportTemplate);
			// get the headers
			GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.HEADERS.getKey());
			String para1 = null;
			String para2 = null;
			
			if(grs != null)
			{
				if(grs.size() > 1)
					para1 = grs.get(0).toString();
				if(grs.size() > 2)
					para2 = grs.get(1).toString();
				
				if(para1 != null)
					exportMap.put("para1", para1);
				if(para2 != null)
					exportMap.put("para2", para2);
			}
			
			// get the gutters 
			if(keyValue.containsKey(ReactorKeysEnum.COLUMN_GUTTER.getKey()))
				this.columnGutter = Integer.parseInt(keyValue.get(ReactorKeysEnum.COLUMN_GUTTER.getKey()));
			else if(exportMap.containsKey(ReactorKeysEnum.COLUMN_GUTTER.getKey()))
				this.columnGutter = Integer.parseInt(exportMap.get(ReactorKeysEnum.COLUMN_GUTTER.getKey()) + "");

			if(keyValue.containsKey(ReactorKeysEnum.ROW_GUTTER.getKey()))
				this.rowGutter = Integer.parseInt(keyValue.get(ReactorKeysEnum.ROW_GUTTER.getKey()));
			else if(exportMap.containsKey(ReactorKeysEnum.COLUMN_GUTTER.getKey()))
				this.rowGutter = Integer.parseInt(exportMap.get(ReactorKeysEnum.ROW_GUTTER.getKey()) + "");
			
		}
		
		
		
		public XSSFWorkbook getWorkBook()
		{
			XSSFWorkbook wb = null;
			if(exportMap.containsKey("FILE_NAME"))
			{
				String fileName = (String)exportMap.get("FILE_NAME");
				
				if(exportMap.containsKey(fileName))
				{
					// keeping a map to be used for various things
					
					wb = (XSSFWorkbook)exportMap.get(fileName);
					
					
					// may be this is not needed for now
					/*
					Object oStartColumn = insight.getVar("COLUMN_COUNT");
					if(oStartColumn != null)
						startColumn = (Integer)oStartColumn;
					*/
				}
				else
				{
					if(exportTemplate != null)
					{
						String fileLocation = (String)exportMap.get("FILE_LOCATION");
						try {
							if (fileLocation == null) {
								// if file location null generate a random path and set location
								String insightFolder = this.insight.getInsightFolder();
								fileLocation = insightFolder + DIR_SEPARATOR + fileName + ".xlsx";
							 }
							 FileUtils.copyFile(new File(exportTemplate), new File(fileLocation));
							 wb = new XSSFWorkbook(fileLocation);
							if(wb.getSheet(FOOTER) != null)
							{
								Sheet aSheet = wb.getSheet(FOOTER);
								if(aSheet.getRow(0) != null)
								{
									Row row = aSheet.getRow(0);
									if(row.getCell(0) != null)
									{
										String footer = row.getCell(0).getStringCellValue();
										exportMap.put(FOOTER, footer);
									}
									
								}
							}
							// check if the sheet contains place holders and export the place holder details.
							if(wb.getSheet(PLACE_HOLDER) != null) {
								Sheet aSheet = wb.getSheet(PLACE_HOLDER);
								if(aSheet.getRow(0) != null) {
									// fetch the updated place holder details from the UI by calling getPlaceHolderDetails method
									Map<String, List<String>> placeholderInfo = getPlaceHolderDetails();
									exportMap.put(PLACE_HOLDER, placeholderInfo);
								}
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else
						wb = new XSSFWorkbook();
					exportMap.put(fileName, wb);
				}
			}
			return wb;
		}
		
		/*
		 * It will retrieve the required sheet from given workbook
		 * @param wb
		 * @param sheetName
		 */
		public XSSFSheet getSheet(XSSFWorkbook wb, String sheetName)
		{
			XSSFSheet aSheet = wb.getSheet(sheetName);
			if (aSheet == null) {
				if (this.exportTemplate != null) {
					// adding a check to throw an error on to the UI if the selected template does
					// not have Header info
					if (wb.getSheetIndex(HEADER) != -1) {
						aSheet = wb.cloneSheet(wb.getSheetIndex(HEADER));
						wb.setSheetName(wb.getSheetIndex(aSheet), sheetName);
						startRow = 6;
						lastRow = 6;
						exportMap.put(sheetName + "ROW_COUNT", startRow);
						// need to find a way to remove disclaimer
					} else {
						// throwing the pixel Exception in case of invalid template
						throw new SemossPixelException(new NounMetadata("Selected template is in invalid format",
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
					}
				} else
					aSheet = wb.createSheet(sheetName);

			} else {
				if (this.exportTemplate != null && wb.getSheetIndex(HEADER) == -1) {
					// throwing the pixel Exception in case of invalid template
					throw new SemossPixelException(new NounMetadata("Selected template is in invalid format",
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
			}
			return aSheet;
		}
		
		public void getMap(String fileName)
		{
			if(insight.getVar(fileName) != null)
			{
				exportMap = (Map)insight.getVar(fileName);
			}
		}

		public void putMap()
		{
			insight.getVarStore().put(insight.getInsightId(), new NounMetadata(exportMap, PixelDataType.CUSTOM_DATA_STRUCTURE));
		}

		public String processTable(String sheetName, String html, String fileName) 
		{
			try
			{
				Document doc = Jsoup.parse(html, "UTF-8");
	
		   	    Workbook wb = null;
				if(exportMap.containsKey(fileName))
				{
					// keeping a map to be used for various things
					
					wb = (Workbook)exportMap.get(fileName);
					
					
					// may be this is not needed for now
					/*
					Object oStartColumn = insight.getVar("COLUMN_COUNT");
					if(oStartColumn != null)
						startColumn = (Integer)oStartColumn;
					*/
				}
				else
				{
					if(exportTemplate != null)
					{
						wb = new XSSFWorkbook(exportTemplate);
						if(wb.getSheet(FOOTER) != null)
						{
							Sheet aSheet = wb.getSheet(FOOTER);
							if(aSheet.getRow(0) != null)
							{
								Row row = aSheet.getRow(0);
								if(row.getCell(0) != null)
								{
									String footer = row.getCell(0).getStringCellValue();
									exportMap.put(FOOTER, footer);
								}
								
							}
						}
					}
					else
						wb = new XSSFWorkbook();
					exportMap.put(fileName, wb);
				}

				Sheet aSheet = null;
			/*
			 * if(exportMap.containsKey(sheetName)) aSheet =
			 * (Sheet)exportMap.get(sheetName); else
			 */	{
				 aSheet = wb.getSheet(sheetName);
				 if(aSheet == null)
				 {
					 if(this.exportTemplate != null)
					 {
						 aSheet = wb.cloneSheet(wb.getSheetIndex(HEADER));
						 wb.setSheetName(wb.getSheetIndex(aSheet), sheetName);
						 startRow = 6;
						 // need to find a way to remove disclaimer
						 
					 }
					 else
						 aSheet = wb.createSheet(sheetName);
					
				 }
				}
				Object oStartRow = exportMap.get(sheetName +"ROW_COUNT");
				
				if(oStartRow != null)
					startRow = (Integer)oStartRow + rowGutter;

		   	    int offset = startRow; 
		   	    lastRow = offset;
		   	    // this assumes a single table being there
				Elements trs = doc.select("tr"); // a with href
				//System.out.println("Table rows " + trs.size());
				for(int rowIndex = 0;rowIndex < trs.size();rowIndex++)
				{
			   	    Row row = aSheet.createRow(offset + rowIndex);
					Element tr = trs.get(rowIndex);
					processRow(wb, row, tr);
					lastRow++;
				}
				// add 2 new lines
				exportMap.put(sheetName + "ROW_COUNT", lastRow);
				if(exportMap.containsKey(sheetName + "COLUMN_COUNT"))
				{
					int curLastColumn = (Integer)exportMap.get(sheetName + "COLUMN_COUNT");
					if(curLastColumn > lastColumn)
						lastColumn = curLastColumn;
				}
				exportMap.put(sheetName + "COLUMN_COUNT", lastColumn);
				exportMap.put("FILE_NAME", fileName);
			    return "Waiting for next command";
			}catch (Exception ex)
			{
				ex.printStackTrace();
			}
			return null;
		}
		
		public void processRow(Workbook wb, Row row, Element tr)
		{
			// get the style for the row if one exists
			Elements tds = tr.children();
			int offset = startColumn + columnGutter;
			lastColumn = offset;
			int tdIndex = 0;
			int cellIndex = 0;
			while(tdIndex < tds.size())
			{
				Cell cell = row.createCell(cellIndex + offset);
				int rowIndex = cell.getRowIndex();
				int colIndex = cell.getColumnIndex();
				if(!colspanMatrix.containsKey(rowIndex + ":" + colIndex) && !(rowspanMatrix.containsKey(rowIndex + ":" + colIndex)))
				{
					// find if the cell already has a value assigned
					// if not see if t
					Element td = tds.get(tdIndex);
					
					String value = td.text();
					String rowSpan = td.attr("rowspan");
					String colSpan = td.attr("colspan");
					
					String style = td.attr("Style");
					List [] nameProps = null;
					if(style == null)
					{
						style = td.attr("style");
					}
					//System.err.println("Value is " + value + " Style is " + style);
					if(style != null)
					{
						nameProps = mapCSS(style);
	
						CellStyle input = wb.createCellStyle();
						// process border
						input = processBorders(input, nameProps[0], nameProps[1]);
						
						// process font
						input = processFont(input, nameProps[0], nameProps[1], wb);
	
						// process alignment
						input = processAlign(input, nameProps[0], nameProps[1]);
						
						// process background
						input = processBackground(input, nameProps[0], nameProps[1]);

						cell.setCellStyle(input);

						// THIS WILL ALSO SET THE CELL VALUE
						formatAndSetCellType(wb, input, cell, value);

						// process background
						processWidth(cell.getSheet(), cell.getColumnIndex(), nameProps[0], nameProps[1]); 
					}
					
					if(rowSpan != null || colSpan != null) {
						processSpan(cell, rowSpan, colSpan, value);
					}
					// if we have a style
					// the format cell type already sets this
					if(style == null) {
						try {
							int intValue = Integer.parseInt(value);
							cell.setCellValue(intValue);
						} catch(Exception ex) {
							try {
								double doubleValue = Double.parseDouble(value);
								cell.setCellValue(doubleValue);							
							} catch (Exception ex2) {
								cell.setCellValue(value);
							}
						}
					}
					tdIndex++;
					cellIndex++;
					lastColumn++;
				}
				else if(!colspanMatrix.containsKey(rowIndex + ":" + colIndex))
				{
					Cell prevCell = (Cell)rowspanMatrix.get(rowIndex + ":" + colIndex);
					cell.setCellStyle(prevCell.getCellStyle());
					cellIndex++;
					//tdIndex++;
				}else //if(!(rowspanMatrix.containsKey(rowIndex + ":" + colIndex)))
				{
					// I have to do something here..
					// I need to paint the same style for this cell also
					Cell prevCell = (Cell)colspanMatrix.get(rowIndex + ":" + colIndex);
					cell.setCellStyle(prevCell.getCellStyle());
					cellIndex++;
				}

				
			}
		}
		
		public List <String> [] mapCSS(String style)
		{
			List <String> names = new Vector<String>();
			List <String> values = new Vector<String>();
			try {
				style = "div { " + style + " ; }";
				StyleSheet sheet = CSSFactory.parseString(style, new URL("http://base.url"));
				//"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left" align="left"
				if(sheet.size() > 0)
				{
					RuleSet rule = (RuleSet)sheet.get(0);
					CombinedSelector [] selector = rule.getSelectors();
					for(int ruleIndex = 0; rule != null && ruleIndex < rule.size();ruleIndex++)
					{
						Declaration cssName = rule.get(ruleIndex);
						//System.err.println("Property....  >> " + cssName.getProperty());
						for(int propIndex = 0;propIndex < cssName.size();propIndex++)
						{
							
							String thisProp = cssName.get(propIndex).toString();
							names.add(cssName.getProperty());
							values.add(thisProp);
						}
					}
				}			
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CSSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return new List[] {names, values};
		}
		
		public String predictName(List <String> names, String cssName, Term value)
		{
			String retName = cssName;
			if(!retName.contains("-"))
			{
				if(value instanceof TermColor)
					retName = retName + "-color";
				if(value instanceof TermLength || value instanceof TermPercent)
				{
					// see if the width is there if not it is the width
					if(!names.contains(retName + "-width"))
						retName = retName + "-width";
					//else
						
				}
				if(value instanceof TermColor)
					retName = retName + "-color";
				if(value instanceof TermColor)
					retName = retName + "-color";
				
			}
			
			
			
			return retName;
		}
		
		// this is not useful
		public void modStyle(Workbook wb, CellStyle input, List <String> cssName, List <String> cssValue)
		{
			 // things we style
			 // background color
			 // font color
			 // font size
			 // font style - italic
			 
			 // font -weight - bold
			 // underline
			 // horizontal justify
			 // vertical justify
			 // borders
			 // width of the cell - you can only do column
			 
			// things I am looking for
			String [] borderProps = new String[] {"border", "border-width", "border-bottom", "border-left", "border-right", "border-top", "border-style"};
			String [] fontProps = new String[] {"font", "font-size", "font-color", "font-family", "font-style", "font-weight"}; // font-style - italic, font-weight - bold
			String [] backgroundProps = new String [] {"background-color"};
			String [] aligns = new String [] {"text-align", "vertical-align"};
			String [] dims = new String [] { "width", "height"}; // auto-wrap
			String [] texts = new String [] {"text-decoration" };// underline
		}
		
		public CellStyle processBorders(CellStyle input, List <String> names, List <String> cssProps)
		{
			String [] borderProps = new String[] {"border", "border-width", "border-bottom", "border-left", "border-right", "border-top", "border-style"};
			
			BorderStyle styleToApply = BorderStyle.THIN;
			
			if(names.contains("border-style"))
			{
				int borderTypeIndex = names.indexOf("border-style");
				String borderType = cssProps.get(borderTypeIndex);
				if(borderType.equalsIgnoreCase("dotted"))
					styleToApply = BorderStyle.DOTTED;
				if(borderType.equalsIgnoreCase("dashed"))
					styleToApply = BorderStyle.DASHED;
				if(borderType.equalsIgnoreCase("double"))
					styleToApply = BorderStyle.DOUBLE;
			}
			
			if(names.contains("border")) // put everything you are good move on
			{
				 input.setBorderBottom(styleToApply);
				 input.setBorderTop(styleToApply);
				 input.setBorderLeft(styleToApply);
				 input.setBorderRight(styleToApply);
			}
			else 
			{
				if(names.contains("border-left"))
					input.setBorderLeft(styleToApply);
				if(names.contains("border-right"))
					input.setBorderRight(styleToApply);
				if(names.contains("border-top"))
					input.setBorderTop(styleToApply);
				if(names.contains("border-bottom"))
					input.setBorderBottom(styleToApply);
			}
			

			
			if(names.contains("border-color"))
			{
				int borderColorIndex = names.indexOf("border-color");
				String borderColor= cssProps.get(borderColorIndex);
				XSSFColor color = new XSSFColor();
				color.setARGBHex(borderColor.substring(1));
				((XSSFCellStyle)input).setBottomBorderColor(color);;
				((XSSFCellStyle)input).setTopBorderColor(color);;
				((XSSFCellStyle)input).setLeftBorderColor(color);;
				((XSSFCellStyle)input).setRightBorderColor(color);;
			}
			else
			{
				input.setBottomBorderColor(IndexedColors.BLACK.getIndex());
				input.setTopBorderColor(IndexedColors.BLACK.getIndex());
				input.setLeftBorderColor(IndexedColors.BLACK.getIndex());
				input.setRightBorderColor(IndexedColors.BLACK.getIndex());
			}
			
			return input;
					
		}
		
		public CellStyle processFont(CellStyle input, List <String> names, List <String> cssProps, Workbook wb)
		{
			String [] fontProps = new String[] {"font", "font-size", "font-family", "font-style", "font-weight"}; // font-style - italic, font-weight - bold
			String [] texts = new String [] {"text-decoration" };// underline
			
			
			 XSSFFont font= (XSSFFont) wb.createFont();
			 int fontSize = 10;
			 boolean bold = false;
			 boolean italic = false;
			 if(names.contains("font-size"))
			 {
					int fontSizeIndex = names.indexOf("font-size");
					String fontSizeStr = cssProps.get(fontSizeIndex);
					
					try {
						fontSize = Integer.parseInt(fontSizeStr.replace("px",""));
					}catch (Exception ignored)
					{}
			 }
			 // font size
		     font.setFontHeightInPoints((short)fontSize);
		     
		     // font name
		     // come back to this
		     font.setFontName("Arial");
		     
			if(names.contains("color"))
			{
				int fIndex = names.indexOf("color");
				String fColor = cssProps.get(fIndex);

				XSSFColor color = new XSSFColor();
				color.setARGBHex(fColor.substring(1));
			     font.setColor(color);
			}
			if(names.contains("font-color"))
			{
				int fIndex = names.indexOf("font-color");
				String fColor = cssProps.get(fIndex);

				XSSFColor color = new XSSFColor();
				color.setARGBHex(fColor.substring(1));
			     font.setColor(color);
			}
			else
		     // color
		     font.setColor(IndexedColors.BLACK.getIndex());

			 if(names.contains("font-weight"))
			 {
					int fontBoldIndex = names.indexOf("font-weight");
					String fontBold = cssProps.get(fontBoldIndex);
					bold = fontBold.equalsIgnoreCase("bold");
			 }
		     font.setBold(bold);

			 if(names.contains("font-style"))
			 {
					int fontStyleIndex = names.indexOf("font-style");
					String fontStyle= cssProps.get(fontStyleIndex);
					italic = fontStyle.equalsIgnoreCase("italic");
			 }
		     font.setItalic(italic);
		     
			 if(names.contains("text-decoration"))
			 {
					int textDecorIndex = names.indexOf("text-decoration");
					String textDecor= cssProps.get(textDecorIndex);
					if(textDecor.equalsIgnoreCase("underline"))
					     font.setUnderline(XSSFFont.U_SINGLE);
			 }

			 input.setFont(font);
	   		 return input;
		}
		
		public CellStyle processAlign(CellStyle input, List <String> names, List <String> cssProps)
		{
			String [] aligns = new String [] {"text-align", "vertical-align"};

			if(names.contains("text-align"))
			 {
					int textAlignIndex = names.indexOf("text-align");
					String textAlign = cssProps.get(textAlignIndex);
					
					if(textAlign.equalsIgnoreCase("left"))
						input.setAlignment(HorizontalAlignment.LEFT);
					if(textAlign.equalsIgnoreCase("center"))
						input.setAlignment(HorizontalAlignment.CENTER);
					if(textAlign.equalsIgnoreCase("right"))
						input.setAlignment(HorizontalAlignment.RIGHT);
			 }
			

			if(names.contains("vertical-align"))
			 {
					int textAlignIndex = names.indexOf("vertical-align");
					String textAlign = cssProps.get(textAlignIndex);
					
					if(textAlign.equalsIgnoreCase("top"))
						input.setVerticalAlignment(VerticalAlignment.TOP);
					if(textAlign.equalsIgnoreCase("middle"))
						input.setVerticalAlignment(VerticalAlignment.CENTER);
					if(textAlign.equalsIgnoreCase("bottom"))
						input.setVerticalAlignment(VerticalAlignment.BOTTOM);
			 }
			
			// wrap the words
			input.setWrapText(true);

			return input;
		}

		public CellStyle processBackground(CellStyle input, List <String> names, List <String> cssProps)
		{
			String [] backgroundProps = new String [] {"background-color"};

			short colorIndex = IndexedColors.WHITE.getIndex();
			if(names.contains("background-color"))
			 {
					int bgIndex = names.indexOf("background-color");
					String bgColor = cssProps.get(bgIndex);
		
					XSSFColor color = new XSSFColor();
					color.setARGBHex(bgColor.substring(1));
				    ((XSSFCellStyle)input).setFillForegroundColor(color);
				    input.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			 }
		     // background
			return input;
		}

		/**
		 * Format and set the cell value
		 * @param wb
		 * @param input
		 * @param cell
		 * @param value
		 */
		public void formatAndSetCellType(Workbook wb, CellStyle input, Cell cell, String value) {
			if(value != null) {
				boolean isNumeric = NumberUtils.isCreatable(value);

				// see if it is $
				if(value.contains("$")) {
					input.setDataFormat((short)0x5);
					Double val = Utility.getDouble(value.replaceAll("[$,\\s]", ""));
					if(val != null) {
						cell.setCellValue(val);
					} else {
						cell.setCellValue(value);
					}
					cell.setCellStyle(input);
				}
				//see if it is a number
				// that starts with 0
				// so treat as a string
				else if(isNumeric && value.startsWith("0")) {
					input.setDataFormat((short)0);
					cell.setCellValue(value);
				// or just a number
				} else if(NumberUtils.isCreatable(value)) {
					if(value.contains(".")) {
						cell.setCellValue(Double.parseDouble(value));
					} else {
						try {
							cell.setCellValue(Integer.parseInt(value));
						} catch(Exception ex) {
							// ignore and set value
							cell.setCellValue(value);
						}
					}
				}
				// see if this is a date yuck
				else {
					input.setDataFormat((short)0x31);
					cell.setCellValue(value);
				}
			}
		}
		
		public void processWidth(Sheet inputSheet, int cellNum, List <String> names, List <String> cssProps)
		{
			String [] widths = new String [] {"width"};

			if(autoWrappedColumns.contains(cellNum)) // already auto wrapped dont mess
			{
				inputSheet.autoSizeColumn(cellNum);			
				return; 
			}
			
			int width = 200;
			if(names.contains("width"))
			 {
					int wIndex = names.indexOf("width");
					String w = cssProps.get(wIndex);
					if(w.equalsIgnoreCase("auto"))
					{
						inputSheet.autoSizeColumn(cellNum);
						autoWrappedColumns.add(cellNum);
						return;
					}
					else if(w.endsWith("px"))
					{
						w = w.replace("px", "");
						width = Integer.parseInt(w);
					}
			 }
			// process to keep the greatest width for this column
			if(columnToWidth.containsKey(cellNum))
			{
				int savedWidth = columnToWidth.get(cellNum);
				if(savedWidth > width)
					width = savedWidth;
			}
			
			// save it
			columnToWidth.put(cellNum, width);

			// convert the width from px to characters. Approx 1 character is 8px.
			width = width/8;
			
			// bound it between 0 and 255
			if(width < 0) {
				width = 0;
			}
			
			if(255 <= width) {
				width = 255;
			}
			
	
			inputSheet.setColumnWidth(cellNum, width * 256);
		}

		public void processSpan(Cell cell, String rowSpan, String colSpan, String value)
		{
			// get the curren cell row and column
			int colIndex = cell.getColumnIndex();
			int rowIndex = cell.getRowIndex();
			
			Sheet sh = cell.getSheet();
			List mergeCells = new Vector();
			if(mergeAreas.containsKey(sh))
				mergeCells = mergeAreas.get(sh);
			
	
			if(colSpan != null && colSpan.length() > 0 && (rowSpan == null || rowSpan.length() == 0))
			{
				int numCols = Integer.parseInt(colSpan);
				for(int colSpanIndex = 0;colSpanIndex < numCols;colSpanIndex++)
					colspanMatrix.put(rowIndex + ":" + (colIndex + colSpanIndex), cell);
				
				// put the merge
				//CellReference.convertNumToColString(cell.getColumnIndex());
				CellRangeAddress cra = new CellRangeAddress(rowIndex, rowIndex, colIndex, (colIndex + numCols - 1));
				mergeCells.add(cra);
				mergeAreas.put(sh, mergeCells);
			}

			if((colSpan == null || colSpan.length() == 0) && rowSpan != null && rowSpan.length() > 0)
			{
				int numRows = Integer.parseInt(rowSpan);
				for(int rowSpanIndex = 0;rowSpanIndex < numRows;rowSpanIndex++)
					rowspanMatrix.put((rowIndex + rowSpanIndex) + ":" + colIndex, cell);
				
				// put the merge
				CellRangeAddress cra = new CellRangeAddress(rowIndex, (rowIndex+numRows-1), colIndex, colIndex);
				mergeCells.add(cra);
				mergeAreas.put(sh, mergeCells);
			}
			
			if(colSpan != null && rowSpan != null && rowSpan.length() > 0 && colSpan.length() > 0)
			{
				int numRows = Integer.parseInt(colSpan);
				int numCols = Integer.parseInt(colSpan);
				for(int rowSpanIndex = 0;rowSpanIndex < numCols;rowSpanIndex++)
				{
					for(int colSpanIndex = 0;colSpanIndex < numCols;colSpanIndex++)
					{
						colspanMatrix.put((rowIndex + rowSpanIndex) + ":" + (colIndex + colSpanIndex), cell);						
						rowspanMatrix.put((rowIndex + rowSpanIndex) + ":" + (colIndex + colSpanIndex), cell);						
					}
					
				}
				
				// put the merge area
				CellRangeAddress cra = new CellRangeAddress(rowIndex, rowIndex+numRows, colIndex, colIndex + numCols);
				mergeCells.add(cra);				
				mergeAreas.put(sh, mergeCells);
			}
			

		}
		
		public void mergeAreas()
		{
			Iterator<Sheet> sheets = mergeAreas.keySet().iterator();
			while(sheets.hasNext())
			{
				Sheet sheet = sheets.next();
				List <CellRangeAddress> mergeCells = mergeAreas.get(sheet);
				for(int mergeIndex = 0;mergeIndex < mergeCells.size();mergeIndex++)
					sheet.addMergedRegion(mergeCells.get(mergeIndex));
			}
		}
		
		public void writeWorkbook(String fileName)
		{
			try {
				Workbook wb = (Workbook) exportMap.get(fileName);
				if(exportMap.containsKey(FOOTER))
					fillFooter(wb, exportMap, (String)exportMap.get(FOOTER));
				fillHeader(wb, exportMap, "Mahers Magic Carpet", "Incurred through abc to def");
				String exportName = AbstractExportTxtReactor.getExportFileName(fileName, "xlsx");
				String fileLocation = "c:/temp" + DIR_SEPARATOR + exportName;

				FileOutputStream fileOut = new FileOutputStream(fileLocation);
				wb.write(fileOut);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		public void fillHeader(Workbook wb, Map exportMap, String para1, String para2)
		{
			// usually the company name is in the first row 4th column
			for(int sheetIndex = 0;sheetIndex < wb.getNumberOfSheets();sheetIndex++)
			{
				Sheet aSheet = wb.getSheetAt(sheetIndex);
				String sheetName = aSheet.getSheetName();

				// should already be there but
				Row para1Row = aSheet.getRow(0);
				if(para1Row == null)
					para1Row = aSheet.createRow(0);
				Row para2Row = aSheet.getRow(1);
				if(para2Row == null)
					para2Row = aSheet.createRow(1);
				
				Cell para1Cell = para1Row.getCell(3);
				Cell para2Cell = para2Row.getCell(3);
				
				if(para1Cell == null) // ok weird
					para1Cell = para1Row.createCell(3);
				if(para2Cell == null) // ok weird
					para2Cell = para2Row.createCell(3);

				CellStyle style = wb.createCellStyle(); //Create new style
	            style.setWrapText(true); //Set wordwrap
	            if(para1 != null)
	            	para1Cell.setCellValue(para1);
	            if(para2 != null)
	            	para2Cell.setCellValue(para2);				
			}
		}
		
		// can neel just send it as part of the information ?
		public void fillFooter(Workbook wb, Map exportMap, String footer)
		{
			wb.removeSheetAt(wb.getSheetIndex(FOOTER));	
			for(int sheetIndex = 0;sheetIndex < wb.getNumberOfSheets();sheetIndex++)
			{
				Sheet aSheet = wb.getSheetAt(sheetIndex);
				String sheetName = aSheet.getSheetName();
				
				// final row count 
				int sheetTotalRows = 0;
				int sheetTotalColumns = 0;
				if(wb.getSheet(HEADER) != null)
					sheetTotalRows = 5; // leave space foe headers
				if(exportMap.containsKey(sheetName + "ROW_COUNT"))
					sheetTotalRows = (Integer)exportMap.get(sheetName + "ROW_COUNT");
				if(exportMap.containsKey(sheetName + "COLUMN_COUNT"))
					sheetTotalColumns = (Integer)exportMap.get(sheetName + "COLUMN_COUNT");
				
				Row row = aSheet.createRow(sheetTotalRows + 2);
				Cell cell = row.createCell(0);
				CellStyle style = wb.createCellStyle(); //Create new style
	            style.setWrapText(true); //Set wordwrap
	            cell.setCellStyle(style); //Apply style to cell
				cell.setCellValue(footer);
				
				aSheet.addMergedRegion(new CellRangeAddress(sheetTotalRows + 2, sheetTotalRows +4, 0, sheetTotalColumns + 4));			
			}
			
		}
		
		
		/** This method will update the placeholder cells with required values.
		 * @param wb
		 * @param exportMap
		 * @param placeHolderData - is a Map that contains Placeholder label as the key and [Placeholder value, placeholder cell position] as the list value.
		 */
		public void fillPlaceholders(Workbook wb, Map exportMap, Map<String, List<String>> placeHolderData) {

			// removing the place holder sheet of the exported workbook
			wb.removeSheetAt(wb.getSheetIndex(PLACE_HOLDER));
			Sheet headerTemplateSheet = wb.getSheet(HEADER);
			for (int sheetIndex = 0; sheetIndex < wb.getNumberOfSheets(); sheetIndex++) {
				Sheet sheet = wb.getSheetAt(sheetIndex);
				String sheetName = sheet.getSheetName();
				// skipping template sheets only to read Data Sheet
				if (sheetName.equalsIgnoreCase(HEADER) || wb.isSheetHidden(sheetIndex)) {
					continue;
				}
				for (List<String> placeHolderValues : placeHolderData.values()) {
					// fetching cell reference/position since index 1 will hold place holder
					// position
					CellReference cellRef = new CellReference(placeHolderValues.get(1));
					int rowIndex = cellRef.getRow();
					int colIndex = cellRef.getCol();
					
					// identifying the required row in the target sheet
					Row row = sheet.getRow(rowIndex);
					// Identifying the required row in the template sheet
					Row headerTemplateRow = headerTemplateSheet.getRow(rowIndex);

					if (row == null) {
						// creating a new target row
						row = sheet.createRow(rowIndex);
					}
					if (headerTemplateRow == null)
						headerTemplateRow = headerTemplateSheet.createRow(rowIndex);

					// Identifying the required cell in the template sheet
					Cell headerTemplateCell = headerTemplateRow.getCell(colIndex);
					// identifying the required cell in the target sheet
					Cell cell = row.getCell(colIndex);
					if (cell == null) {
						// creating a new target cell
						cell = row.createCell(colIndex);
					}
					// retain the cell style provided in the header template
					CellStyle headCellStyle = getPreferredCellStyle(headerTemplateCell);
					cell.setCellStyle(headCellStyle);
					// fetching place holder value since index 0 will hold the value
					String resolvedValue = resolvePlaceHolderValue(placeHolderValues.get(0));
					cell.setCellValue(resolvedValue);
				}
			}
			// removing the header sheet of the workbook during export
			wb.removeSheetAt(wb.getSheetIndex(HEADER));
		}
		
		/** This method will resolve the place holder value parameters with dynamic filter values
		 * @param placeHolderValue
		 * @return
		 */
		private String resolvePlaceHolderValue(String placeHolderValue) {
			// the dynamic params with format <param> will be resolved
			Pattern pattern = Pattern.compile("<[a-z_0-9]+>", Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(placeHolderValue);
			Iterator<String> insightParamKeys = insight.getVarStore().getInsightParameterKeys().iterator();

			while (matcher.find()) {
				// fetching the value if parameter from ExportParamutility
				String paramName = matcher.group();
				paramName = paramName.replace("<", "");
				paramName = paramName.replace(">", "");
				
				// ok.. now I have the param name as if insight has it
				NounMetadata param = insight.getVarStore().get(VarStore.PARAM_STRUCT_PREFIX + paramName);
				String result = null;
				if(param != null)
				{
					ParamStruct ps = (ParamStruct)param.getValue();
					// going to get just the first psd and get data from that
					result = ps.getDetailsList().get(0).getCurrentValue() + "";
				}
				//String result = ExportParamUtility.getParamValue(matcher.group(), insight);
				if (result != null) //  && !result.isEmpty() - empty can be legit value
				{
					placeHolderValue = placeHolderValue.replaceAll(matcher.group(), result);
				}
			}
			return placeHolderValue;
		}

		/** This method will capture the place holder details from Pixel parameter
		 * place holder is a bookmark tagged to a cell with a name, default value and cell value. 
		 * intent is to dynamically place the content
		 * @return
		 */
		private Map<String, List<String>> getPlaceHolderDetails() {
			GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PLACE_HOLDER_DATA.getKey());
			if (grs != null && !grs.isEmpty()) {
				List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
				if (mapInput != null && !mapInput.isEmpty()) {
					// return the updated place holder information as map with key as place holder
					// label and place holder value, cell position as value
					return (Map<String, List<String>>) mapInput.get(0);
				}
			}

			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if (mapInput != null && !mapInput.isEmpty()) {
				// return the updated place holder information as map with key as place holder
				// label and place holder value, cell position as value
				return (Map<String, List<String>>) mapInput.get(0);
			}

			return null;
		}
		
		/** This method to get the preferred cell style for a cell
		 * @param cell
		 * @return
		 */
		public CellStyle getPreferredCellStyle(Cell cell) {
			CellStyle cellStyle = cell.getCellStyle();
			if (cellStyle.getIndex() == 0)
				cellStyle = cell.getRow().getRowStyle();
			if (cellStyle == null)
				cellStyle = cell.getSheet().getColumnStyle(cell.getColumnIndex());
			if (cellStyle == null)
				cellStyle = cell.getCellStyle();
			return cellStyle;
		}
		/*
		 * It will add the parameters to excel file as new sheet
		 * @param wb
		 * @param insight
		 * @param applyDefaultColor
		 * @param exportMap-it stores all the export related properties
		 */
		public void makeParamSheet(Workbook wb, Insight insight, boolean applyDefaultColor, Map exportMap) 
		{
			
			String SECTION = "Section"; // This column various section of filter like Insight, Frame
			String PARAMETER_NAME = "Parameter Name";
			String OPERATOR = "Operator"; // Operator used in UI translated to English word
			String PARAMETER_VALUE = "Parameter Value(s)";
			int START_COLUMN_INDEX = 0;
			String AUDIT_SHEET_NAME= "Audit Parameters";
			int startRowIndex = 0;

			// creating a new Audit Parameters sheet applying the template
			Sheet paramSheet = getSheet((XSSFWorkbook)wb,AUDIT_SHEET_NAME);
			//fetching the start ROW_COUNT after applying the template
			Object startRow = exportMap.get(AUDIT_SHEET_NAME + "ROW_COUNT");				
			if(startRow != null)
				startRowIndex = ((Integer)startRow)+1;	
			
			Row row = paramSheet.createRow(startRowIndex);

			// print all the headers
			Cell paramNameCellHeader = row.createCell(START_COLUMN_INDEX + 1);
			Cell operatorCellHeader = row.createCell(START_COLUMN_INDEX + 2);
			Cell paramValCellHeader = row.createCell(START_COLUMN_INDEX + 3);
			// applying style to the heading
			if (applyDefaultColor) {
				CellStyle cellHeaderStyle = getCellStyle(wb);
				paramNameCellHeader.setCellStyle(cellHeaderStyle);
				paramValCellHeader.setCellStyle(cellHeaderStyle);
				operatorCellHeader.setCellStyle(cellHeaderStyle);
			}
			// Header text being appended
			paramNameCellHeader.setCellValue(PARAMETER_NAME);
			operatorCellHeader.setCellValue(OPERATOR);
			paramValCellHeader.setCellValue(PARAMETER_VALUE);
			// fill the rows. 
			// Ideally we can drop the section, but later
			Iterator<String> insightParamKeys = insight.getVarStore().getInsightParameterKeys().iterator();

			int rowIndex = startRowIndex + 1;
			while(insightParamKeys.hasNext())
			{
				String paramName = insightParamKeys.next();
				if (paramName != null) 
				{
					// extracting parameter structure from meta data
					ParamStruct insightParamStruct = (ParamStruct) insight.getVarStore().get(paramName).getValue();
					for (ParamStructDetails paramStructDetails : insightParamStruct.getDetailsList()) 
					{
						row = paramSheet.createRow(rowIndex);
						// Friendly name given to param from UI
						String friendlyParamName = paramStructDetails.getColumnName();
						String paramValue = paramStructDetails.getCurrentValue() != null
								? paramStructDetails.getCurrentValue() + ""
								: null;
						String operator = paramStructDetails.getOperator();
						if(operator == null || operator.length() == 0)
							operator = "=="; // forcing equals
						
						// set the values
						Cell paramNameCell = row.createCell(START_COLUMN_INDEX + 1);
						paramNameCell.setCellValue(friendlyParamName);					
						Cell paramOperatorCell = row.createCell(START_COLUMN_INDEX + 2);
						paramOperatorCell.setCellValue(IQueryFilter.getDisplayNameForComparator(operator));
						Cell paramValueCell = row.createCell(START_COLUMN_INDEX + 3);
						paramValueCell.setCellValue(paramValue);

						// next row
						rowIndex++;

						// set eh styles
					}
				}
			}
			// auto sizing the columns after adding all the values to it
			for (int colmnIndex = START_COLUMN_INDEX; colmnIndex < START_COLUMN_INDEX+4; colmnIndex++) {
				paramSheet.autoSizeColumn(colmnIndex);
			}
			//storing the ROW_COUNT,COLUMN_COUNT information in map for appending footer
			exportMap.put(paramSheet.getSheetName() + "ROW_COUNT", rowIndex);
			exportMap.put(paramSheet.getSheetName() + "COLUMN_COUNT", START_COLUMN_INDEX+4);

		}

		// default color and font to the audit sheet headers
		private static CellStyle getCellStyle(Workbook wb) {
			// create a CellStyle with the font
			CellStyle headerCellStyle = wb.createCellStyle();
			XSSFFont font = (XSSFFont) wb.createFont();
			XSSFColor fontColor = new XSSFColor();
			fontColor.setARGBHex("ffffff");
			font.setColor(fontColor);
			headerCellStyle.setFont(font);
			// Cell Background color
			XSSFColor color = new XSSFColor();
			color.setARGBHex("00a8c1");
			((XSSFCellStyle) headerCellStyle).setFillForegroundColor(color);
			headerCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			return headerCellStyle;

		}

		
		public static void main(String [] args)
		{
			TableToXLSXReactor tx = new TableToXLSXReactor();
			String html = "</head><body><table style=\"border-collapse:collapse; margin:50px auto; width:750px\" width=\"750\">\r\n" + 
					"  <thead>\r\n" + 
					"    <tr>\r\n" + 
					"      <th style=\"background-color:#3498db; color:white; font-weight:bold; border:1px solid #ccc; font-size:18px; padding:10px; text-align:left; width:80px\" align=\"left\">First Name</th>  <th style=\"background-color:#3498db; color:white; font-weight:bold; border:1px solid #ccc; font-size:18px; padding:10px; text-align:left; width:20px\" align=\"left\">Last Name</th>\r\n" + 
					"      <th style=\"background-color:#3498db; color:white; font-weight:bold; border:1px solid #ccc; font-size:18px; padding:10px; text-align:left; width:20px\" align=\"left\">Job Title</th>\r\n" + 
					"      <th style=\"background-color:#3498db; color:white; font-weight:bold; border:1px solid #ccc; font-size:18px; padding:10px; text-align:left; width:20px\" align=\"left\">Twitter</th>\r\n" + 
					"    </tr>\r\n" + 
					"  </thead>\r\n" + 
					"  <tbody>\r\n" + 
					"    <tr>\r\n" + 
					"      <td data-column=\"First Name\"  style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\", colspan=\"2\">James</td>\r\n" + 
					"      <td data-column=\"Last Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Matman</td>\r\n" + 
					"      <td data-column=\"Job Title\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Chief Sandwich Eater</td>\r\n" + 
					"      <td data-column=\"Twitter\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">20</td>\r\n" + 
					"    </tr>\r\n" + 
					"    <tr>\r\n" + 
					"      <td data-column=\"First Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">30.123</td>\r\n" + 
					"      <td data-column=\"Last Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Nagy</td>\r\n" + 
					"      <td data-column=\"Job Title\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Designer</td>\r\n" + 
					"      <td data-column=\"Twitter\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">@andornagy</td>\r\n" + 
					"    </tr>\r\n" + 
					"    <tr>\r\n" + 
					"      <td data-column=\"First Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Tamas</td>\r\n" + 
					"      <td data-column=\"Last Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Biro</td>\r\n" + 
					"      <td data-column=\"Job Title\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Game Tester</td>\r\n" + 
					"      <td data-column=\"Twitter\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">$ 2000.32</td>\r\n" + 
					"    </tr>\r\n" + 
					"    <tr>\r\n" + 
					"      <td data-column=\"First Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Zoli</td>\r\n" + 
					"      <td data-column=\"Last Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Mastah</td>\r\n" + 
					"      <td data-column=\"Job Title\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Developer</td>\r\n" + 
					"      <td data-column=\"Twitter\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">@zoli</td>\r\n" + 
					"    </tr>\r\n" + 
					"    <tr>\r\n" + 
					"      <td data-column=\"First Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Szabi</td>\r\n" + 
					"      <td data-column=\"Last Name\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Nagy</td>\r\n" + 
					"      <td data-column=\"Job Title\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">Chief Sandwich Eater</td>\r\n" + 
					"      <td data-column=\"Twitter\" style=\"border:1px solid #ccc; font-size:18px; padding:10px; text-align:left\" align=\"left\">@szabi</td>\r\n" + 
					"    </tr>\r\n" + 
					"  </tbody>\r\n" + 
					"</table></body></html>";
			tx.exportTemplate = "c:/users/pkapaleeswaran/workspacej3/SemossDev/templates/anthem.xlsx";
			tx.processTable("sh", html, "hello");
			tx.mergeAreas();
			tx.writeWorkbook("hello");
			
			
		}
		
		
		
}
