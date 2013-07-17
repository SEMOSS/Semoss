package prerna.poi.main;

import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JList;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class POIReader {

	static Hashtable enumHash = new Hashtable();
	Properties rdfMap = new Properties(); // rdf mapping DBCM Prop
	Properties bdProp = new Properties();// properties for big data
	public static String RELATION = "BaseData";
	public static String CONTAINS = "Contains";
	public SailConnection sc = null;
	Sail bdSail = null;
	ValueFactory vf = null;
	Logger logger = Logger.getLogger(getClass());
	//TAPDataScrambler tapDS = new TAPDataScrambler();
	String customBaseURI = null;
	Hashtable baseRelationsHash = new Hashtable();//base relations
	public Hashtable createdURIsHash = new Hashtable();//new object uris
	public String baseObjRelations = "";
	public String baseRelRelations = "";
	public Hashtable createdRelURIsHash = new Hashtable();//new relationship uris
	public String dbPropURI="";
	public String dbPredicateURI="";
	public POIReader importReader;

	
	/*
	 * High level notes ==== ===== ===== Class level data a.k.a MOF Level 3 ---------------------------------- - Create
	 * Statement <Class Name> <rdf:type> <rdfs:class> Property level - Same as class -
	 * 
	 * There will be a master level node class and master level relationship class
	 * 
	 * Instance Level Data a.k.a. MOF Level 4 -------------------------------------- Nodes ----- - Label -
	 * http://x/subspace/<type of node>#<instance name> - Create Statement http://x/subspace/<type of node>/<instance
	 * name> RDFS:Label <instance_name> - For each property - Find the namespace for the given property name - search
	 * using the property name - Results in <property> - Create statement http://x/subspace/<property>#<instance name> -
	 * Create Statement <Label> <property> actual instance data - Need to find a way a specify instance of - <label>
	 * rdf:type <type_of_node>
	 * 
	 * Relationships ------------- - Node 1 - http://x/subspace/<type of node>#<instance name> - Node 2 -
	 * http://x/subspace/<type of node>#<instance name> - Get the relationship from the properties file - Find the name
	 * space by utilizing the pattern <type of node1>_relationshipName_<type of node2> - Results in <relationship> -
	 * Relationship namespace - http://x/subspace/<relationship> - Create Statement <Node1> <relationship> <Node2> - For
	 * each property - Find the namespace for the given property name - search using the property name - Results in
	 * <property> - Create statement http://x/subspace/<property>#<instance name> - Create Statement <relationship>
	 * <property> actual instance data - Need to specify a way of instance - <label> rdf:type <property> - Dont know if
	 * we need to do this here at master level I think we can create it we just dont need to create it again and again
	 */

	public static void main(String[] args) throws Exception {
		// try to load the file and see the worksheets

		String workingDir = System.getProperty("user.dir");
		String propFile = "";//DO NOT EDIT HERE---it is now specified in the loops below, depending on what db you are loading
		String bdPropFile = "";//DO NOT EDIT HERE---it is now specified in the loops below, depending on what db you are loading
		ArrayList<String> files = new ArrayList<String>();
		
		
		//UPDATE THESE FOUR THINGS TO SPECIFY WHAT YOU WANT/HOW TO LOAD::::::::::::::::::::::::::::::::::
		String customBase = "http://semoss.org/ontologies";
		boolean runCoreLoadingSheets = false;
		boolean runFinancialLoadingSheets = false;
		boolean runCustomLoadSheets = false;
		

		if(runCoreLoadingSheets){
			propFile = workingDir + "/RDF_Map.prop";
			bdPropFile = workingDir + "/db/common/BigData.Properties";
			
			String coreFile1 = workingDir + "/Version_5main.xlsm";
			files.add(coreFile1);
			String coreFile2 = workingDir + "/Version_5p2.xlsx";
			files.add(coreFile2);
			String coreFile3 = workingDir + "/Version_5ser.xlsx";
			files.add(coreFile3);
			String coreFile4 = workingDir + "/Version_5req.xlsx";
			files.add(coreFile4);
			String coreFile5 = workingDir + "/DataElementsLoadSheet.xlsx";
			files.add(coreFile5);	
			String coreFile6 = workingDir + "/TransitionCostLoadingSheetsv2.xlsx";
			files.add(coreFile6);
		}
			
		if(runFinancialLoadingSheets){
			propFile = workingDir + "/RDF_Map.prop";
			bdPropFile = workingDir + "/db/financial/CostData.Properties";

			String financialFile1 = workingDir + "/LoadingSheets1.xlsx";
			files.add(financialFile1);
			String financialFile2 = workingDir + "/TransitionCostLoadingSheetsv2.xlsx";
			files.add(financialFile2);
			String financialFile3 = workingDir + "/Site_HWSW.xlsx";
			files.add(financialFile3);
			String financialFile4 = workingDir + "/AncillaryGLItems.xlsx";
			files.add(financialFile4);
			String financialFile5 = workingDir + "/SDLCLoadingSheets.xlsx";
			files.add(financialFile5);
			String financialFile6 = workingDir +"/PFFinancialLoadingSheets2.xlsx";
			files.add(financialFile6);
		}
			
		if(runCustomLoadSheets){
			propFile = workingDir + "/RDF_Map.prop";
			bdPropFile = workingDir + "/db/financial/CostData.Properties";

			String fileName1 = workingDir + "/CustomSheet.xlsx";
			files.add(fileName1);
		}
				
		
		POIReader reader = new POIReader();
		if(customBase!=null) reader.customBaseURI = customBase;
		reader.loadBDProperties(bdPropFile);
		reader.openDB();
		//if customBaseURI is null, load just as before--using the prop file to discover Excel->URI translation
		if(reader.customBaseURI==null)
			reader.loadProperties(propFile);
		
		for(String fileName : files){
			reader.importFile(fileName);
		}
		
		reader.createBaseRelations();
		reader.closeDB();
	}

	public void importFileWithConnection(String engineName, String fileNames, String customBase, String customMap) throws Exception {
		//convert fileNames to array list of files-------all fileNames separated by ";" as defined in FileBrowseListener
		String[] files = fileNames.split(";");
		
		String propFile = "";
		if(!customMap.equals("")) propFile = customMap;
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);

		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
		BigDataEngine bigEngine= (BigDataEngine) engine;
		// String db = workingDir+"/dummyTemp.db";
		importReader = new POIReader();
		if(!customBase.equals("")) importReader.customBaseURI = customBase;
		importReader.bdSail = bigEngine.bdSail;
		importReader.sc = bigEngine.sc;
		importReader.vf = bigEngine.vf;
	
		//if user selected a map, load just as before--using the prop file to discover Excel->URI translation
		if(!propFile.equals(""))
			importReader.loadProperties(propFile);
		
		for(String fileName : files){
			importReader.importFile(fileName);
		}
		
		importReader.createBaseRelations();		
		importReader.sc.commit();
	}
	
	public void importFileWithOutConnection(String dbName, String fileNames, String customBase, String customMap) throws Exception {
		//convert fileNames to array list of files-------all fileNames separated by ";" as defined in FileBrowseListener
		String[] files = fileNames.split(";");
		
		String propFile = "";
		if(!customMap.equals("")) propFile = customMap;
		String bdPropFile = dbName;

		importReader = new POIReader();
		if(!customBase.equals("")) importReader.customBaseURI = customBase;
		importReader.loadBDProperties(bdPropFile);
		importReader.openDB();

		//if user selected a map, load just as before--using the prop file to discover Excel->URI translation
		if(!propFile.equals(""))
			importReader.loadProperties(propFile);
		
		for(String fileName : files){
			importReader.importFile(fileName);
		}
		
		importReader.createBaseRelations();
		importReader.closeDB();
	}

	public void createBaseRelations() throws Exception{
		if(rdfMap.containsKey(RELATION)){ //load using what is on the map
			String value = rdfMap.getProperty(RELATION);
			System.out.println(" Relations are " + value);
			StringTokenizer relTokens = new StringTokenizer(value, ";");
			while (relTokens.hasMoreTokens()) {
				String rel = relTokens.nextToken();
				String relNames = rdfMap.getProperty(rel);
				StringTokenizer rdfTokens = new StringTokenizer(relNames, ";");
				int count = 0;
				while (rdfTokens.hasMoreTokens()) {
					StringTokenizer stmtTokens = new StringTokenizer(
							rdfTokens.nextToken(), "+");
					String subject = stmtTokens.nextToken();
					String predicate = stmtTokens.nextToken();
					String object = stmtTokens.nextToken();
					count++;
	
					// create the statement now
					createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));
	
				}// statement while
			}// relationship while
		}//if using map
		if(baseRelationsHash.size()>0) { //in addition to whats on the map, need to also load all in baseRelationsHash which should contain all necessary subclass information
			Iterator baseHashIt = baseRelationsHash.keySet().iterator();
			//here I'll create the necessary subclass triples required every time.  Might already be include above, but that is ok
			String sub = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS;
			String pred = RDF.TYPE.stringValue();
			String obj = Constants.CLASS_URI;
			createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			baseObjRelations = baseObjRelations+sub+"+"+pred+"+"+obj+";";
			sub =  customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS;
			pred = RDF.TYPE.stringValue();
			obj = Constants.DEFAULT_PROPERTY_URI;
			createStatement(vf.createURI(sub), vf.createURI(pred), vf.createURI(obj));
			baseRelRelations = baseRelRelations+sub+"+"+pred+"+"+obj+";";
			if(!dbPropURI.equals(""))
				baseRelRelations = baseRelRelations+dbPropURI+"+"+Constants.SUBPROPERTY_URI+"+"+dbPropURI+";";
			
			//now add all of the base relations tha have been stored in the hash.
			while(baseHashIt.hasNext()){
				String subjectInstance = baseHashIt.next() +"";
				String objectInstance = baseRelationsHash.get(subjectInstance) +"";
				
				//predicate depends on whether its a relation or a node
				String predicate = "";
				if(objectInstance.equals(Constants.DEFAULT_NODE_CLASS)) predicate = Constants.SUBCLASS_URI;
				else if (objectInstance.equals(Constants.DEFAULT_RELATION_CLASS)) predicate = Constants.SUBPROPERTY_URI;
				
				//convert instances to URIs
				String subject = customBaseURI + "/" + objectInstance + "/" + subjectInstance;
				String object = customBaseURI + "/" + objectInstance;

				// create the statement now
				createStatement(vf.createURI(subject), vf.createURI(predicate), vf.createURI(object));

				if(objectInstance.equals(Constants.DEFAULT_NODE_CLASS))
					baseObjRelations = baseObjRelations+subject+"+"+predicate+"+"+object+";";
				else if(objectInstance.equals(Constants.DEFAULT_RELATION_CLASS))
					baseRelRelations = baseRelRelations+subject+"+"+predicate+"+"+object+";";
			}
		}
	}

	public void openDB() throws Exception {

		bdSail = new BigdataSail(bdProp);
		Repository repo = new BigdataSailRepository((BigdataSail) bdSail);
		repo.initialize();
		SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();
		sc = src.getSailConnection();
		
		vf = bdSail.getValueFactory();

	}

	public void closeDB() throws Exception
	{
		//ng.stopTransaction(Conclusion.SUCCESS);
        InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
        ie.computeClosure(null);

		sc.commit();
		sc.close();
		bdSail.shutDown();
		//ng.shutdown();
	}

	public void loadProperties(String fileName) throws Exception {
		rdfMap.load(new FileInputStream(fileName));
	}
	
	public void loadBDProperties(String fileName) throws Exception
	{
		bdProp.load(new FileInputStream(fileName));

	}

	public void importFile(String fileName) throws Exception {

		XSSFWorkbook book = new XSSFWorkbook(new FileInputStream(fileName));

		// System.out.println("Number of sheets " + book.getNumberOfSheets());
		// System.out.println("Sheet Name ::::: " + book.getSheetAt(0).getSheetName());

		// load the sheets to be loaded first

		XSSFSheet lSheet = book.getSheet("Loader");

		// assumption is all the sheets are in the first column starting from row 2

		// need a procedure here to load the base relationships first

		int lastRow = lSheet.getLastRowNum();
		for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
			// get thr sheet first
			XSSFRow row = lSheet.getRow(rIndex);
			XSSFCell cell = row.getCell(0);
			XSSFCell cell2 = row.getCell(1);
			if (cell != null && cell2 != null) {
				String sheetToLoad = cell.getStringCellValue();
				System.out.println("Cell Content is " + sheetToLoad);
				// this is a relationship
				if (cell2 != null
						&& cell2.getStringCellValue().contains("Matrix")) {
					if (cell2.getStringCellValue().contains("Dynamic"))
						loadMatrixSheet(sheetToLoad, book, true);
					else
						loadMatrixSheet(sheetToLoad, book, false);
				} else if (cell2 != null
						&& cell2.getStringCellValue().contains("Dynamic"))
					loadSheet(sheetToLoad, book, true);
				else
					loadSheet(sheetToLoad, book, false);
			}
		}
	}

	public void loadSheet(String sheetToLoad, XSSFWorkbook book, boolean dynamic) throws Exception{

		XSSFSheet lSheet = book.getSheet(sheetToLoad);

		// assumption is all the sheets are in the first column starting from row 2

		int lastRow = lSheet.getLastRowNum()+1;

		// System.out.println("Last row " + systemSheet.getLastRowNum());
		// Get the first row to get column names
		XSSFRow row = lSheet.getRow(0);
		System.out.println("Max columns " + row.getLastCellNum());

		// get the column names
		String data = null;
		int count = 0;
		String idxName = null;
		String nodeType = null;
		String otherIdx = null;
		Vector propNames = new Vector();
		String relName = null;

		nodeType = row.getCell(0).getStringCellValue();
		idxName = row.getCell(1).getStringCellValue();
		int curCol = 1;
		if (nodeType.equalsIgnoreCase("Relation")) {
			otherIdx = row.getCell(2).getStringCellValue();
			curCol++;
		}

		// adds all the property names
		// if relationship then starts with 2 else starts at 1, starting column is 0
		// loads it into vector propNames
		for (int colIndex = curCol + 1; colIndex < row.getLastCellNum(); propNames
				.addElement(row.getCell(colIndex).getStringCellValue()), colIndex++)
			;

		// now process the remaining nodes
		// finally the graph db YAY !!
		//org.neo4j.graphdb.Transaction tx = svc.beginTx();
		try {
			System.out.println("Last Row is " + lastRow);

			// processing starts here
			for (int rowIndex = 1; rowIndex < lastRow; rowIndex++) {
				//System.out.println("Processing " + rowIndex);
				// first cell is the name of relationship
				XSSFRow nextRow = lSheet.getRow(rowIndex);

				// get the name of the relationship
				if (rowIndex == 1)
					relName = nextRow.getCell(0).getStringCellValue();

				// get the name of the node
				String thisNode = null;
				if (nextRow.getCell(1) != null
						&& nextRow.getCell(1).getCellType() != XSSFCell.CELL_TYPE_BLANK)
					nextRow.getCell(1).setCellType(Cell.CELL_TYPE_STRING);
					thisNode = nextRow.getCell(1).getStringCellValue();
				// get the second element - this is the name
				String otherNode = null;
				Hashtable propHash = new Hashtable();
				int startCol = 2;
				int offset = 2;
				if (nodeType.equalsIgnoreCase("Relation")) {
					nextRow.getCell(2).setCellType(Cell.CELL_TYPE_STRING);
					otherNode = nextRow.getCell(2).getStringCellValue();
					startCol++;
					offset++;
				}

				// System.out.println(" ROw Index " + rowIndex);
				for (int colIndex = startCol; colIndex < nextRow
						.getLastCellNum(); colIndex++) {
					// System.out.println(colIndex + "<<>>" + nextRow.getLastCellNum());
					if(propNames.size() <= (colIndex-offset)) {
						continue;
					}
					String propName = (String) propNames.elementAt(colIndex
							- offset);
					String propValue = null;
					if (nextRow.getCell(colIndex) == null || nextRow.getCell(colIndex).getCellType() == XSSFCell.CELL_TYPE_BLANK || nextRow.getCell(colIndex).toString().isEmpty()) {
						continue;
					}
					if (nextRow.getCell(colIndex).getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
						if(DateUtil.isCellDateFormatted(nextRow.getCell(colIndex))){
							Date date = (Date) nextRow.getCell(colIndex).getDateCellValue();
							propHash.put(propName, date);
						}
						else{
							Double dbl = new Double(nextRow.getCell(colIndex)
									.getNumericCellValue());
							// propValue = nextRow.getCell(colIndex).getNumericCellValue() + "";
							propHash.put(propName, dbl);
						}
					} else {
						propValue = nextRow.getCell(colIndex)
								.getStringCellValue();
						propHash.put(propName, propValue);
					}
				}

				if (nodeType.equalsIgnoreCase("Relation")) {
					//System.out.println("Adding " + thisNode + "<<>>"
						//	+ otherNode + "<<>>" + rowIndex);
					System.out.println("Processing " + sheetToLoad + " Row " + rowIndex);
					if (!dynamic)
						addRelation(idxName, otherIdx, thisNode, otherNode,
								relName, propHash);
					else
						System.out.println("Method for loading dynamic relationship does not exist");
				} else
					addNode(idxName, thisNode, propHash);
				//tx.success();
				count++;
			}
		} finally {
			//tx.finish();
		}
	}
	
	private String cleanString(String original, boolean replaceForwardSlash){
		String retString = original;
		retString = retString.trim();
		retString = retString.replaceAll(" ", "_");//replace spaces with underscores
		retString = retString.replaceAll("\"", "'");//replace double quotes with single quotes
		if(replaceForwardSlash)retString = retString.replaceAll("/", "-");//replace forward slashes with dashes
		retString = retString.replaceAll("\\|", "-");//replace vertical lines with dashes
		
		boolean doubleSpace = true;
		while (doubleSpace == true)//remove all double spaces
		{
			doubleSpace = retString.contains("  ");
			retString = retString.replace("  ", " ");
		}
		
		return retString;
	}

	// need to write routine to add relationship
	public void addRelation(String idxName, String otherIdx, String nodeName,
			String nodeName2, String relName, Hashtable propHash) throws Exception{
		nodeName = cleanString(nodeName, true);
		nodeName2 = cleanString(nodeName2, true);
		
		// 1. find the URI name for the index name
		// 2. find the URI for the other index
		// 3. Append the nodename and create a statement nodenameURI <typeof> of that node
		// 4. Do the same for nodename2
		// 5. Get the relationship based on the predicate for relName - Rel Predicate
		// 6. Create a new URI with <rel predicate>/nodeName~nodeName2 - Specific Predicate
		// 7. Create a subpropertyof relationship i.e. the <specific predicate> subclassof <predicate> - the reason we
		// need to do this is we need to hang the properties of it
		// 8. Create statement nodename <predicate> nodename2
		// 9. Now all that is left is properties
		// 10. For each property - Get the attribute URI
		// 11. Create statement this node <typeof> attribute
		// 12. Hang this property of relationship
		// 13. <rel predicate>/nodeName~nodeName2 <has predicate> <property URI>
		
		// TODO : Need to associate some name or a literal for this
		// Use the RDFS.LABEL

		// 1. find the URI name for the index name
		String idxURI = "";
		if(rdfMap.containsKey(idxName)) idxURI = rdfMap.getProperty(idxName);
		else {//if we are using customBaseURI, must create the URI from scratch and store idxName in baseRelationsHash for creation of base relations at the end of this process
			idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ idxName;
			baseRelationsHash.put(idxName, Constants.DEFAULT_NODE_CLASS);
			createdURIsHash.put(idxName, idxURI);
		}

		// 2. find the URI for the other index
		String idx2URI = "";
		if(rdfMap.containsKey(otherIdx)) idx2URI = rdfMap.getProperty(otherIdx);
		else {//if we are using customBaseURI, must create the URI from scratch and store idxName in baseRelationsHash for creation of base relations at the end of this process
			idx2URI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ otherIdx;
			baseRelationsHash.put(otherIdx, Constants.DEFAULT_NODE_CLASS);
			createdURIsHash.put(otherIdx, idx2URI);
		}
		//RDFS.

		// 3. Append the nodename and create a statement nodenameURI <typeof> of that node
		
		//nodeName = tapDS.processName(nodeName, idxName);
		String labelName = nodeName;
		String nodeURI = idxURI + "/" + nodeName;
		createStatement(vf.createURI(nodeURI), RDF.TYPE, vf.createURI(idxURI));
		createStatement(vf.createURI(nodeURI), RDFS.LABEL, vf.createLiteral(labelName));

		// 4. Do the same for nodename2
		
		//nodeName2 = tapDS.processName(nodeName2, otherIdx);
		String label2Name = nodeName2;
		String node2URI = idx2URI + "/" + nodeName2;
		createStatement(vf.createURI(node2URI), RDF.TYPE, vf.createURI(idx2URI));
		createStatement(vf.createURI(node2URI), RDFS.LABEL, vf.createLiteral(label2Name));

		relName = cleanString(relName, true);
		// 5. Get the relationship based on the predicate for relName - Rel Predicate
		String relURI = "";
		if(rdfMap.containsKey(idxName + "_"+ relName + "_" + otherIdx)) {
			relURI = rdfMap.getProperty(idxName + "_"+ relName + "_" + otherIdx);
			if(dbPredicateURI.equals("")) dbPredicateURI = relURI.substring(0, relURI.lastIndexOf("/"));
		}
		else {//if we are using customBaseURI, must create URI and store relName for creation of base relations
			relURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + relName;
			baseRelationsHash.put(relName, Constants.DEFAULT_RELATION_CLASS);
			createdRelURIsHash.put(idxName + "_"+ relName + "_" + otherIdx, relURI);
			if(dbPredicateURI.equals("")) dbPredicateURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		}
		// 6. Create a new URI with <rel predicate>/nodeName~nodeName2 - Specific Predicate
		String thisRelURI = relURI + "/" + nodeName + Constants.RELATION_URI_CONCATENATOR + nodeName2;

		// 7. Create a subpropertyof relationship i.e. the <specific predicate> subclassof <predicate> - the reason we
		// need to do this is we need to hang the properties of it
		createStatement(vf.createURI(thisRelURI), RDFS.SUBPROPERTYOF, vf.createURI(relURI));
		createStatement(vf.createURI(thisRelURI), RDFS.LABEL, vf.createLiteral(labelName + Constants.RELATION_URI_CONCATENATOR + label2Name));

		// 8. Create statement nodename <predicate> nodename2
		createStatement(vf.createURI(nodeURI), vf.createURI(thisRelURI), vf.createURI(node2URI));
		Enumeration propKeys = propHash.keys();
		while (propKeys.hasMoreElements()) {
			// process the properties
			// 9. Now all that is left is properties
			// 10. For each property - Get the attribute URI
			// 11. Create statement this node <typeof> attribute - Not sure if we need this, we can get away with having
			// a special type of relationship, we can just use the contains
			if(dbPropURI.equals("")){
				if(rdfMap.containsKey(CONTAINS)) dbPropURI = rdfMap.getProperty(CONTAINS);
				else dbPropURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			String key = (String)propKeys.nextElement();
			String propURI = dbPropURI + "/" + key;
			if(propHash.get(key).getClass() == new Double(1).getClass())
			{
				Double value = (Double)propHash.get(key);
				//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
				createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(dbPropURI));				
				createStatement(vf.createURI(thisRelURI), vf.createURI(propURI), vf.createLiteral(value.doubleValue()));
			}
			else if(propHash.get(key).getClass() == new Date(1).getClass())
			{
				Date value = (Date)propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
				createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(dbPropURI));	
				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
				createStatement(vf.createURI(thisRelURI), vf.createURI(propURI), vf.createLiteral(date, datatype));
			}
			else
			{
				String value = (String)propHash.get(key);
				// try to see if it already has properties then add to it
				String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");
				//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
				createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(dbPropURI));				
				createStatement(vf.createURI(thisRelURI), vf.createURI(propURI), vf.createLiteral(cleanValue));

				// 13. <rel predicate>/nodeName~nodeName2 <property> <property URI>
				// createStatement(thisRelURI, propURI, value);
			}				
		}
	}

	private void createStatement(URI subject, URI predicate, Value object) throws Exception
	{
		//System.out.println("TRIPLE --  " + subject + "<>" + predicate + "<>" + object);
		
		URI newSub = null;
		URI newPred = null;
		Value newObj = null;
		String subString = null;
		String predString = null;
		String objString = null;
		String sub = subject.stringValue().trim();
		String pred = predicate.stringValue().trim();
				
		subString = cleanString(sub, false);
		newSub = vf.createURI(subString);
		
		predString = cleanString(pred, false);
		newPred = vf.createURI(predString);
		
		if(object instanceof Literal) 
			newObj = object;
		else {
			objString = cleanString(object.stringValue(), false);
			newObj = vf.createURI(objString);
		}
		
		sc.addStatement(newSub, newPred, newObj);
	}

	// need to write routine to adding dynamic relationship

	// need to write routine to add node
	public void addNode(String indexName, String nodeName, Hashtable propHash) throws Exception{
		nodeName = cleanString(nodeName, true);
		String labelName = nodeName;
		
		// same as relationship
		// 1. Find the indexName from rdfMap - typeURI
		// 2. Create a new URI for the specific - typeURI/nodeName
		// 3. For each property - Get the attribute URI
		// 4. Create statement this propertyURI <subpropertyof> attribute
		// 5. Hang this property of typeURI/nodeName
		// 6. typeURI/nodeName <propertyURI> value
		
		// 1. Find the indexName from rdfMap - typeURI
		String idxURI = "";
		if(rdfMap.containsKey(indexName)) idxURI = rdfMap.getProperty(indexName);
		else {
			idxURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS +"/"+ indexName;
			baseRelationsHash.put(indexName, Constants.DEFAULT_NODE_CLASS);
			createdURIsHash.put(indexName, idxURI);
		}

		
		// 2. Create a new URI for the specific - typeURI/nodeName
		String nodeURI = idxURI + "/" + nodeName;

		
		createStatement(vf.createURI(nodeURI), RDF.TYPE,vf.createURI(idxURI));
		createStatement(vf.createURI(nodeURI), RDFS.LABEL, vf.createLiteral(labelName));

		// 3. For each property - Get the attribute URI
		// 4. Create statement this propertyURI <subpropertyof> attribute
		// 5. Hang this property of typeURI/nodeName
		// 6. typeURI/nodeName <propertyURI> value
		Enumeration propKeys = propHash.keys();
		while (propKeys.hasMoreElements()) {
			// process the properties
			// 9. Now all that is left is properties
			// 10. For each property - Get the attribute URI
			// 11. Create statement this node <typeof> attribute - Not sure if we need this, we can get away with having
			// a special type of relationship, we can just use the contains
			if(dbPropURI.equals("")){
				if(rdfMap.containsKey(CONTAINS)) dbPropURI = rdfMap.getProperty(CONTAINS);
				else dbPropURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/" + CONTAINS;
			}
			String key = (String)propKeys.nextElement();
			String propInstanceURI = dbPropURI + "/" + key;
			if(propHash.get(key).getClass() == new Double(1).getClass())
			{
				Double value = (Double)propHash.get(key);
				//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
				createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(dbPropURI));				
				createStatement(vf.createURI(nodeURI), vf.createURI(propInstanceURI), vf.createLiteral(value.doubleValue()));
			}
			else if(propHash.get(key).getClass() == new Date(1).getClass())
			{
				Date value = (Date)propHash.get(key);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(value);
				//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
				createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(dbPropURI));	
				URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
				createStatement(vf.createURI(nodeURI), vf.createURI(propInstanceURI), vf.createLiteral(date, datatype));
			}
			else
			{
				String value = (String)propHash.get(key);
				if(value.equals(Constants.PROCESS_CURRENT_DATE)){
					insertCurrentDate(propInstanceURI, dbPropURI, nodeURI);
				}
				else if(value.equals(Constants.PROCESS_CURRENT_USER)){
					insertCurrentUser(propInstanceURI, dbPropURI, nodeURI);
				}
				else{
					// try to see if it already has properties then add to it
					String cleanValue = cleanString(value, true);
					//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
					createStatement(vf.createURI(propInstanceURI), RDF.TYPE, vf.createURI(dbPropURI));				
					createStatement(vf.createURI(nodeURI), vf.createURI(propInstanceURI), vf.createLiteral(cleanValue));
					// 13. <rel predicate>/nodeName~nodeName2 <property> <property URI>
					// createStatement(thisRelURI, propURI, value);
				}
			}				
		}
	}
	
	private void insertCurrentUser(String propURI, String containsURI, String nodeURI){
		String cleanValue = System.getProperty("user.name");
		//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
		try{
			createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(containsURI));				
			createStatement(vf.createURI(nodeURI), vf.createURI(propURI), vf.createLiteral(cleanValue));
		} catch (Exception e) {
			logger.error(e);
		}	
	}
	
	private void insertCurrentDate(String propURI, String containsURI, String nodeURI){
		Date dValue = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String date = df.format(dValue);
		//createStatement(vf.createURI(propURI), RDFS.SUBPROPERTYOF, vf.createURI(containsURI));
		try {
			createStatement(vf.createURI(propURI), RDF.TYPE, vf.createURI(containsURI));
			URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
			createStatement(vf.createURI(nodeURI), vf.createURI(propURI), vf.createLiteral(date, datatype));
		} catch (Exception e) {
			logger.error(e);
		}	
		
	}

	public void loadMatrixSheet(String sheetToLoad, XSSFWorkbook book,
			boolean dynamic) throws Exception{

		// these sheets are typically of the form
		// First column - entity 1
		// First Row - entity 2 - Need to find how to get this entity name, may be by tokenizing with hiphen in between

		XSSFSheet lSheet = book.getSheet(sheetToLoad);

		// assumption is all the sheets are in the first column starting from row 2

		int lastRow = lSheet.getLastRowNum();

		// System.out.println("Last row " + systemSheet.getLastRowNum());
		// Get the first row to get column names
		XSSFRow row = lSheet.getRow(0);
		System.out.println("Max columns " + row.getLastCellNum());

		// get the column names
		String data = null;
		int count = 0;
		String idxName = null;
		String nodeType = null;
		String otherIdx = null;
		Vector propNames = new Vector();
		String relName = null;

		nodeType = row.getCell(0).getStringCellValue();
		String complexName = row.getCell(1).getStringCellValue();
		StringTokenizer tokens = new StringTokenizer(complexName, "-");
		idxName = tokens.nextToken();
		int curCol = 1;
		if (nodeType.equalsIgnoreCase("Relation")) {
			otherIdx = tokens.nextToken();
		}

		// load all the columns first
		Vector colNames = new Vector();

		for (int colIndex = curCol + 1; colIndex < row.getLastCellNum(); colNames
				.addElement(row.getCell(colIndex).getStringCellValue()), colIndex++)
			;

		//org.neo4j.graphdb.Transaction tx = svc.beginTx();
		try {

			// now process the remaining nodes
			for (int rowIndex = 1; rowIndex < lastRow; rowIndex++) {
				// first cell is the name of relationship
				XSSFRow nextRow = lSheet.getRow(rowIndex);

				// get the name of the relationship
				if (rowIndex == 1)
					relName = nextRow.getCell(0).getStringCellValue();

				// get the name of the node
				String thisNode = nextRow.getCell(1).getStringCellValue();
				// get the second element - this is the name
				// need to run through all of the columns and put the value
				for (int colIndex2 = curCol + 1, cnIndex = 0; colIndex2 < nextRow
						.getLastCellNum() && cnIndex < colNames.size(); colIndex2++, cnIndex++) {
					Hashtable propHash = new Hashtable();
					String otherNode = (String) colNames.elementAt(cnIndex);
					// XSSFCell.
					if (nextRow.getCell(colIndex2).getCellType() == XSSFCell.CELL_TYPE_NUMERIC)
						propHash.put("weight",
								new Double(nextRow.getCell(colIndex2)
										.getNumericCellValue()));
					// finally the graph db YAY !!
					if (nodeType.equalsIgnoreCase("Relation"))
						addRelation(idxName, otherIdx, thisNode, otherNode,
								relName, propHash);
					else
						addNode(idxName, thisNode, propHash);
					//tx.success();
				}
			}
		} finally {
			//tx.finish();
		}
	}
}
