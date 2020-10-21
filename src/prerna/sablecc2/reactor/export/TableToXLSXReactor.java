package prerna.sablecc2.reactor.export;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class TableToXLSXReactor	extends AbstractReactor
{

		List <Integer> autoWrappedColumns = new Vector<Integer>();
		int startColumn = 0;
		int startRow = 0;
		int rowOffset = 2;
		int columnOffset = 2;
		int lastRow = startRow;
		int lastColumn = startColumn;
		boolean keepOpen = true;
		Map exportMap = null;
		
		public static final String ROW_COUNT = "ROW_COUNT";
		public static final String COLUMN_COUNT = "COLUMN_COUNT";
		public TableToXLSXReactor() {
			// keep open specifies whether to keep this open or close it. if kept open then this will return open as noun metadata
			this.keysToGet = new String[] { ReactorKeysEnum.SHEET.getKey(), ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_NAME.getKey()};
		}

		public NounMetadata execute()
		{
			organizeKeys();
			

			String fileName = Utility.getRandomString(5);
			String html = null;
			String sheetName = null;

			getMap(insight.getInsightId());

			if(keyValue.containsKey(keysToGet[1]))
				html = keyValue.get(keysToGet[1]);
			if(keyValue.containsKey(keysToGet[0]))
				sheetName = keyValue.get(keysToGet[0]);
			if(keyValue.containsKey(keysToGet[2]))
				fileName = keyValue.get(keysToGet[2]);
			else if(exportMap.containsKey("FILE_NAME"))
				fileName = (String)exportMap.get("FILE_NAME");


			// process the table
			// set in insight so FE can download the file
			String fileLocation = processTable(sheetName, html,  fileName);
			NounMetadata retNoun = null;
			retNoun = new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
			
			return retNoun;
		}
		
		public void getMap(String fileName)
		{
			if(insight.getVar(fileName) != null)
			{
				exportMap = (Map)insight.getVar(fileName);
			}
			else
				exportMap = new HashMap<String, Object>();
		}

		public String processTable(String sheetName, String html, String fileName) 
		{
			String fileLocation = null;
			try
			{
				Document doc = Jsoup.parse(html, "UTF-8");
	
		   	    Workbook wb = null;
				if(exportMap.containsKey(fileName))
				{
					// keeping a map to be used for various things
					
					wb = (Workbook)exportMap.get(fileName);
					
					Object oStartRow = exportMap.get(sheetName +"ROW_COUNT");
					
					if(oStartRow != null)
						startRow = (Integer)oStartRow;
					
					// may be this is not needed for now
					/*
					Object oStartColumn = insight.getVar("COLUMN_COUNT");
					if(oStartColumn != null)
						startColumn = (Integer)oStartColumn;
					*/
				}
				else
				{
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
					aSheet = wb.createSheet(sheetName);
					//exportMap.put(sheetName, aSheet);
				}
		   	    int offset = startRow + rowOffset; 
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
				exportMap.put(sheetName + "ROW_COUNT", lastRow);
				exportMap.put("FILE_NAME", fileName);
				insight.getVarStore().put(insight.getInsightId(), new NounMetadata(exportMap, PixelDataType.CUSTOM_DATA_STRUCTURE));
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
			int offset = startColumn + columnOffset;
			lastColumn = offset;
			for(int tdIndex = 0;tdIndex < tds.size();tdIndex++)
			{
				Cell cell = row.createCell(tdIndex + offset);
				Element td = tds.get(tdIndex);
				String value = td.text();
				cell.setCellValue(value);
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

					// process background
					processWidth(cell.getSheet(), cell.getColumnIndex(), nameProps[0], nameProps[1]); 

				}
				lastColumn++;
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

		public void processWidth(Sheet inputSheet, int cellNum, List <String> names, List <String> cssProps)
		{
			String [] widths = new String [] {"width"};

			if(autoWrappedColumns.contains(cellNum)) // already auto wrapped dont mess
			{
				inputSheet.autoSizeColumn(cellNum);			
				return; 
			}
			
			int width = 10;
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
			inputSheet.setColumnWidth(cellNum, width *256);
		}


}
