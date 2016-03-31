package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import prerna.util.Utility;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class CSVFileHelper {
	
	CsvParser parser = null;
	CsvParserSettings settings = null;
	FileReader sourceFile = null;
	String fileName = null;
	public String [] allHeaders = null;
	char delimiter = ',';
	String [] curHeaders = null;
	Hashtable <String, String> cleanDirtyMapper = new Hashtable<String, String>();
	Hashtable <String, String> dirtyTypeMapper = new Hashtable<String, String>();
	
	public static void main(String [] args) throws Exception
	{
		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		long before, after;
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Remedy Try.csv";
		before = System.nanoTime();
		CSVFileHelper test = new CSVFileHelper();
		test.parse(fileName);
		test.printRow(test.getRow());
		test.printRow(test.allHeaders);
		test.allHeaders = null;
		test.reset();
		test.printRow(test.allHeaders);
		System.out.println(test.countLines());
		String [] columns = {"Status", "Exceeded"};
		test.parseColumns(columns);
		test.reset();
		System.out.println(test.countLines());
		//test.printRow(test.getRow());
		after = System.nanoTime();
		System.out.println((after - before)/1000000);
	}

	public void makeSettings()
	{
		settings = new CsvParserSettings();
    	settings.setNullValue("");
    	settings.getFormat().setDelimiter(delimiter);
        settings.setEmptyValue(""); // for CSV only
        settings.setSkipEmptyLines(true);
	}
	
	public void parse(String fileName)
	{
		this.fileName = fileName;
		makeSettings();
		createParser();
	}
		
	private void createParser()
	{
    	parser = new CsvParser(settings);
    	try {
			File file = new File(fileName);
			sourceFile = new FileReader(file);
			parser.beginParsing(sourceFile);
			//parser.beginParsing(file);
			collectHeaders();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void collectHeaders()
	{
		if(allHeaders == null)
			allHeaders = getRow();
		for(int headIndex = 0;headIndex < allHeaders.length;headIndex++)
			this.cleanDirtyMapper.put(cleanString(allHeaders[headIndex]), allHeaders[headIndex]);
	}
	
	private String cleanString(String original)
	{
		return Utility.cleanString(original, true);
	}
	
	public void parseColumns(String [] columns)
	{
		// map it back to cleann columns
		String [] unCleanColumns = new String[columns.length];
		for(int colIndex = 0;colIndex < columns.length;colIndex++)
			unCleanColumns[colIndex] = cleanDirtyMapper.get(columns[colIndex]);
		makeSettings();
		settings.selectFields(columns);
		curHeaders = columns;
		reset();
	}
	
	public String[] getRow()
	{
		return parser.parseNext();
	}
	
	// resets everything.. just something to watch
	public void reset()
	{
		try {
			sourceFile.close();
			parser.stopParsing();
			createParser();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void reset(boolean getHeader)
	{
		reset();
		if(!getHeader)
			getRow(); // do nothing with it
	}
	
	public int countLines()
	{
		int count = 0;
		while(getRow() != null)
			count++;
		
		return count;
	}
	
	public int getColumns()
	{
		int colCount = 0;
		if(curHeaders != null)
			colCount = curHeaders.length;
		
		return colCount;
	}
	
	
	public void printRow(String [] data)
	{
		for(int dataIndex = 0;dataIndex < data.length;dataIndex++)
			System.out.print("["+data[dataIndex] + "]");
		
		System.out.println("----");
	}

	public void setDelimiter(char charAt) {
		// TODO Auto-generated method stub
		this.delimiter = charAt;
		
	}
	
	
}
