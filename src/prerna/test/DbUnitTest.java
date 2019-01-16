package prerna.test;

import java.io.File;

import org.dbunit.dataset.ITable;
import org.dbunit.dataset.excel.XlsDataSet;

public class DbUnitTest {

	public static void main(String [] args) throws Exception
	{
		//InputStream fis = new FileInputStream(new File("c:/users/pkapaleeswaran/workspacej3/datasets/Movie.xls"));
		
		XlsDataSet xl = new XlsDataSet(new File("c:/users/pkapaleeswaran/workspacej3/datasets/Movie.xlsx"));
		
		// print the xl data
		String [] tables = xl.getTableNames();
	
		for(int tableIndex =0;tableIndex < tables.length;tableIndex++)
			System.out.println("table > " + tables[tableIndex]);
		
		ITable table = xl.getTable("Movie");
		System.out.println(table.getRowCount() + " <<>> " + table.getTableMetaData().getColumns().length);
		
		
		
		
	}
	
}
