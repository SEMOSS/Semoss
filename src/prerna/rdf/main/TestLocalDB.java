package prerna.rdf.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TestLocalDB {

	public static void main(String[] args) throws IOException {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data.smss";
		BigDataEngine tapCore = new BigDataEngine();
		tapCore.openDB(engineProp);
		tapCore.setEngineName("TAP_Core_Data");
		DIHelper.getInstance().setLocalProperty("TAP_Core_Data", tapCore);

		List<Object[]> triples = new ArrayList<Object[]>();
		
		Set<String> systemNames = new HashSet<String>();
		String csvFileLocation = "C:\\Users\\mahkhalil\\Desktop\\For Maher.xlsx";
		FileInputStream poiReader = new FileInputStream(csvFileLocation);
		XSSFWorkbook workbook = new XSSFWorkbook(poiReader);
		// load the Loader tab to determine which sheets to load
		XSSFSheet lSheet = workbook.getSheet("Sheet1");
		int lastRow = lSheet.getLastRowNum();
		for (int rIndex = 0; rIndex <= lastRow; rIndex++) {
			XSSFRow row = lSheet.getRow(rIndex);
			if (row != null) {
				XSSFCell cell = row.getCell(0);
				if(cell != null) {
					systemNames.add(Utility.cleanString(cell.getStringCellValue(), true));
				}
			}
		}
		
		String query = "SELECT DISTINCT ?System ?SustainmentBudget WHERE { {?System a <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBudget} } ";

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(tapCore, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			Object sub = ss.getRawVar(names[0]);
			String cleanSub = ss.getVar(names[0]) + "";
			Object pred = "http://semoss.org/ontologies/Relation/Contains/SustainmentBudget";
			Object obj = ss.getVar(names[1]);

			if(systemNames.contains(cleanSub)) {
				System.out.println(sub + " >>> " + pred + " >>>" + obj);
				triples.add(new Object[]{sub, pred, obj, false});
			}
		}
		
		System.out.println(triples.size());
		
		for(Object[] trip : triples) {
			tapCore.doAction(ACTION_TYPE.REMOVE_STATEMENT, trip);
		}
		tapCore.commit();
	}

}
