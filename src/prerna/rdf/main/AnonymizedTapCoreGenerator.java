package prerna.rdf.main;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;


public class AnonymizedTapCoreGenerator {

	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
	private static final Logger classLogger = LogManager.getLogger(AnonymizedTapCoreGenerator.class);

//	public static void main(String[] args) throws Exception {
//		// adding all the required paths up front here
//		String rdfMapLocation = "C:\\workspace\\Semoss_Dev\\RDF_Map.prop";
//		String tapCoreSmss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data__133db94b-4371-4763-bff9-edf7e5ed021b.smss";
//		String tapSiteSmss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Site_Data__eed12b32-bc38-4718-ab73-c0c78480c174.smss";
//		String tapPortfolioSmss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Portfolio__4254569c-3e78-4d62-8a07-1f786edf71e6.smss";
//		
//		// write to both json and excel
//		// excel for business users
//		// json for developers
//		String matchingFileJson = "C:\\workspace\\Semoss_Dev\\TAP_ANONYMIZED_MATCHING.json";
//		String matchingFileExcel = "C:\\workspace\\Semoss_Dev\\TAP_ANONYMIZED_MATCHING.xlsx";
//
//		// even if run for TAP CORE is false
//		// it will still query TAP Core to get the ultimate list of systems
//		// so make sure the path for TAP Core is accurate
//		boolean runForTapCore = true;
//		boolean runForTapSite = true;
//		boolean runforTapPortfolio = true;
//		// if this is true, make sure the directory for the json and excel paths is accurate
//		boolean createMatchingFile = true;
//		
//		TestUtilityMethods.loadAll(rdfMapLocation);
//
//		List<String> systemReplacementOrder = new Vector<String>();
//		Map<String, String> systemMapping = new HashMap<String, String>();
//
//		{
//			BigDataEngine engine = new BigDataEngine();
//			engine.open(tapCoreSmss);
//
//			// get a list of all the systems
//			SelectQueryStruct qs = new SelectQueryStruct();
//			qs.addSelector(new QueryColumnSelector("System"));
//			qs.addOrderBy("System");
//
//			int counter = 1;
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow data = wrapper.next();
//				Object[] row = data.getValues();
//				String system = row[0].toString();
//				systemMapping.put(system, "System" + counter);
//				counter++;
//
//				// keep track of all systems
//				// will order this so we know what to replace when
//				systemReplacementOrder.add(system);
//			}
//
//			//System.out.println(systemMapping.size());
//			//System.out.println(gson.toJson(systemMapping));
//
//			// order the systems from largest to smallest
//			systemReplacementOrder.sort(new Comparator<String>() {
//				@Override
//				public int compare(String o1, String o2) {
//					if(o1.length() > o2.length()) {
//						return -1;
//					} else if(o1.length() < o2.length()) {
//						return 1;
//					}
//					return 0;
//				}
//			});
//
//			//System.out.println(gson.toJson(systemReplacementOrder));
//
//			if(runForTapCore) {
//				runReplacementForEngine(engine, systemReplacementOrder, systemMapping);
//			}
//		}
//		
//		if(runForTapSite) {
//			BigDataEngine engine = new BigDataEngine();
//			engine.open(tapSiteSmss);
//			runReplacementForEngine(engine, systemReplacementOrder, systemMapping);
//		}
//		
//		if(runforTapPortfolio) {
//			BigDataEngine engine = new BigDataEngine();
//			engine.open(tapPortfolioSmss);
//			runReplacementForEngine(engine, systemReplacementOrder, systemMapping);
//		}
//
//		if(createMatchingFile) {
//			// we will make both a JSON and an Excel
//			
//			// start with json as it is easy
//			String prettyJson = GSON.toJson(systemMapping);
//			Path path = Paths.get(matchingFileJson);
//			Files.write(path, prettyJson.getBytes());
//
//			// excel, need to loop through
//			
//			SXSSFWorkbook workbook = new SXSSFWorkbook(1000);
//			SXSSFSheet sheet = workbook.createSheet("Mappings");
//			sheet.setRandomAccessWindowSize(100);
//			// freeze the first row
//			sheet.createFreezePane(0, 1);
//			
//			// create the header row
//	        Row headerRow = sheet.createRow(0);
//			// create a Font for styling header cells
//			Font headerFont = workbook.createFont();
//			headerFont.setBold(true);
//			// create a CellStyle with the font
//			CellStyle headerCellStyle = workbook.createCellStyle();
//			headerCellStyle.setFont(headerFont);
//	        headerCellStyle.setAlignment(HorizontalAlignment.CENTER);
//	        headerCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
//
//			// generate the header row
//			// and define constants used throughout like size, and types
//	        Cell origNameCellH = headerRow.createCell(0);
//	        origNameCellH.setCellValue("Original System Name");
//	        origNameCellH.setCellStyle(headerCellStyle);
//			
//	        Cell newNameCellH = headerRow.createCell(1);
//	        newNameCellH.setCellValue("Anonymized System Name");
//	        newNameCellH.setCellStyle(headerCellStyle);
//	        
//	        // row counter
//	        int rowCounter = 1;
//			for(String origSystem : systemMapping.keySet()) {
//				Row dataRow = sheet.createRow(rowCounter);
//				dataRow.createCell(0).setCellValue(origSystem);
//				dataRow.createCell(1).setCellValue(systemMapping.get(origSystem));
//				
//				// update the row
//				rowCounter++;
//			}
//			
//			// Write the output to a file
//			FileOutputStream fileOut = null;
//			try {
//				fileOut = new FileOutputStream(matchingFileExcel);
//				workbook.write(fileOut);
//				workbook.close();
//				workbook.dispose();
//			} catch (IOException e) {
//				e.printStackTrace();
//			} finally {
//				if (fileOut != null) {
//					try {
//						fileOut.close();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//				if (workbook != null) {
//					try {
//						workbook.close();
//						workbook.dispose();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//			}
//		}
//	}

	private static void runReplacementForEngine(BigDataEngine engine, List<String> systemReplacementOrder, Map<String, String> systemMapping) {
		List<Object[]> removeTriples = new Vector<Object[]>();
		List<Object[]> addTriples = new Vector<Object[]>();

		System.out.println("Staring execution for " + engine.getEngineName());
		int counter = 0;

		String query = "select ?s ?p ?o where {"
				+ "{?s ?p ?o}"
				+ "}";

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			while(wrapper.hasNext()) {
				IHeadersDataRow data = wrapper.next();
				Object[] row = data.getValues();
				Object[] raw = data.getRawValues();

				String rawSub = raw[0].toString();
				String rawPred = raw[1].toString();

				String origSub = row[0].toString();
				String origPred = row[1].toString();
				Object origObj = row[2];
				boolean objIsString = (origObj instanceof String);
				boolean objIsUri = objIsString && raw[2].toString().startsWith("http://");

				String cleanSub = origSub;
				String cleanPred = origPred;
				Object cleanObj = origObj;

				// have to loop for all systems
				// since things like system interfaces may have more than
				// 1 system appear twice
				for(String system : systemReplacementOrder) {
					String replacementSystem = systemMapping.get(system);

					// do the replacements
					if(cleanSub.contains(system)) {
						cleanSub = cleanSub.replace(system, replacementSystem);
					}
					if(cleanPred.contains(system)) {
						cleanPred = cleanPred.replace(system, replacementSystem);
					}
					if(objIsString && cleanObj.toString().contains(system)) {
						cleanObj = cleanObj.toString().replace(system, replacementSystem);
					}
				}

				if(!cleanSub.equals(origSub) || !cleanPred.equals(origPred) || !cleanObj.equals(origObj)) {
					// need to delete this 
					// and add a new triple
					if(objIsUri) {
						removeTriples.add(new Object[] {rawSub, rawPred, raw[2].toString(), true});
					} else {
						removeTriples.add(new Object[] {rawSub, rawPred, origObj, false});
					}

					String baseSub = rawSub.substring(0, rawSub.lastIndexOf('/'));
					String basePred = rawPred.substring(0, rawPred.lastIndexOf('/'));
					if(objIsUri) {
						// URI
						String baseObj = raw[2].toString().substring(0, raw[2].toString().lastIndexOf('/'));
						addTriples.add(new Object[] {baseSub + cleanSub, basePred + cleanPred, baseObj + "/" + cleanObj, true});
					} else {
						// literal
						addTriples.add(new Object[] {baseSub + cleanSub, basePred + cleanPred, cleanObj, false});
					}
				}

				if(++counter % 10_000 == 0) {
					System.out.println("Finished " + counter + " triple checks");
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		System.out.println("Done execution");

		System.out.println("Removing " + engine.getEngineName() + " Triples");
		System.out.println("Number of replacements = " + removeTriples.size());
	}

}
