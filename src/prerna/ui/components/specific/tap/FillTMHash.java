/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
/**
 * This fills the hashtable for technical maturity calculations.
 */
public class FillTMHash extends GridPlaySheet{

	String query = null;
	SesameJenaSelectWrapper sjsw = null;
	IEngine engine = null;
	public Hashtable<String, Object> TMhash = new Hashtable<String, Object>();
	SesameJenaSelectWrapper wrapper;
	ArrayList <String []> list;
	GridFilterData gfd = new GridFilterData();
	
	Logger fileLogger = Logger.getLogger("reportsLogger");
	static final Logger logger = LogManager.getLogger(FillCapabilityBVHash.class.getName());
	
	//Necessary items to create a filler
	/**
	 * Sets the engine and query.
	 * @param query String
	 * @param engine IEngine
	 */
	public void setEngine (String query,
			IEngine engine) {
		this.query = query;
		this.engine = engine;
	}
	
	/**
	 * Creates the first view of any playsheet.
	 */
	@Override
	public void createView(){
	}
	/**
	 * Runs through all of the queries given pertaining to system software and hardware for the selected database.
	 * Calculate the technical maturity.
	 */
	public void runQueries()
	{
		//String MultipleQueries = this.sparql.getText();
		//first queries are create queries which count the rows to see how big of a matrix is required.  
		//Second queries are fill queries which fill the created matrices
		String MultipleQueries = "SELECT ('CREATE' AS ?CREATE_MATRIX) ('SystemSoftwareModule' AS ?SystemSoftwareMLifecycle) ('NA' AS ?type) (COUNT(DISTINCT(?sys)) AS ?sysCount) (COUNT(DISTINCT(?softM)) AS ?softMCount) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?softM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;} {?softV <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;}{?sys ?has ?softM ;} {?softM ?typeOf ?softV ;}OPTIONAL{{?category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVCategory> ;}{?softV <http://semoss.org/ontologies/Relation/Has> ?category}}OPTIONAL{?softV <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVlifePot ;}OPTIONAL{?softM <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMlifePot ;}}"+"+++"+
		"SELECT ('CREATE' AS ?CREATE_MATRIX) ('SystemSoftwareModule' AS ?SystemSoftwareMCategory) ('NA' AS ?type) (COUNT(DISTINCT(?sys)) AS ?sysCount) (COUNT(DISTINCT(?softM)) AS ?softMCount) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?softM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;} {?softV <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;} {?sys ?has ?softM ;} {?softM ?typeOf ?softV ;}OPTIONAL{{?category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVCategory> ;}{?softV <http://semoss.org/ontologies/Relation/Has> ?category}}OPTIONAL{?softV <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVlifePot ;}OPTIONAL{?softM <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMlifePot ;}}"+"+++"+
		"SELECT DISTINCT ('FILL' AS ?FILL_MATRIX) ('SystemSoftwareModuleLifecycle' AS ?SystemSoftwareMLifecycle) ('Lifecycle' AS ?type) ?sys ?softM (COALESCE(xsd:dateTime(?SoftMlifePot), COALESCE(xsd:dateTime(?SoftVlifePot),'TBD')) AS ?life) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?softM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;} {?softV <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;}{?sys ?has ?softM ;} {?softM ?typeOf ?softV ;}OPTIONAL{{?category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVCategory> ;}{?softV <http://semoss.org/ontologies/Relation/Has> ?category}}OPTIONAL{?softV <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVlifePot ;}OPTIONAL{?softM <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMlifePot ;}}"+"+++"+ 
		"SELECT DISTINCT ('FILL' AS ?FILL_MATRIX) ('SystemSoftwareModuleCategory' AS ?SystemSoftwareMCategory) ('Category' AS ?type) ?sys ?softM (COALESCE(?category,'Platform') AS ?cat) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?softM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;} {?softV <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;}{?sys ?has ?softM ;} {?softM ?typeOf ?softV ;}OPTIONAL{{?category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVCategory> ;}{?softV <http://semoss.org/ontologies/Relation/Has> ?category}}OPTIONAL{?softV <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVlifePot ;}OPTIONAL{?softM <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMlifePot ;}}"+"+++"+
		"SELECT DISTINCT ('CREATE' AS ?CREATE_MATRIX) ('SystemHardware' AS ?SystemHardware) ('NA' AS ?type) (COUNT(DISTINCT(?System)) AS ?sysCount) (COUNT(DISTINCT(?Hardwarem)) AS ?hardCount) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}{?Hardwarem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>;}{?System ?has ?Hardwarem.}{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;}{?Hardwarev <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>;}{?Hardwarem ?typeof ?Hardwarev;}OPTIONAL{?Hardwarev <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardVlifePot ;}OPTIONAL{?Hardwarem <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardMlifePot ;}}"+"+++"+
		"SELECT DISTINCT ('FILL' AS ?FILL_MATRIX) ('SystemHardware' AS ?SystemHardware) ('Hardware' AS ?type) ?System ?Hardwarem (COALESCE(xsd:dateTime(?HardMlifePot), COALESCE(xsd:dateTime(?HardVlifePot),'TBD')) AS ?life) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}{?Hardwarem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>;}{?System ?has ?Hardwarem.}{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;}{?Hardwarev <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>;}{?Hardwarem ?typeof ?Hardwarev;}OPTIONAL{?Hardwarev <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardVlifePot ;}OPTIONAL{?Hardwarem <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardMlifePot ;}}";
		StringTokenizer queryTokens = new StringTokenizer(MultipleQueries, "+++");
		boolean failed = false;
		String failedQuery = null;//this is used to see if the engine is missing information.  Then we can set a error pop up.
		DIHelper.getInstance().setLocalProperty(Constants.TECH_MATURITY, TMhash);
		
		//set the engine to selected database
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repos = (Object [])list.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
		logger.info("Repository is " + repos);
		
		//run through all queries
		while(queryTokens.hasMoreElements()){
			String query = (String) queryTokens.nextElement();
			boolean check = Utility.runCheck(query);//here is the utility check
			if(check==false){
				failed = true;
				failedQuery = query;
				break;
			}
			setEngine(query, (IEngine)engine);
			fillWithSelect();

		}
		
		
		
		if(failed == true){
			jBar.setVisible(false);
			runFailPopup(failedQuery);
			fileLogger.info("Did not return any results for Tech Maturity query: "+query);
		}
		else{
			TMCalculationPerformer calculator = new TMCalculationPerformer();
			TMhash = calculator.tmCalculate();
		}
	}
	
	/**
	 * Popup window that appears when a query fails to run because the necessary information is unavailable in the RDF store.
	 * @param failedQuery String
	 */
	public void runFailPopup(String failedQuery){
		int loc = failedQuery.indexOf("WHERE");
		String queryIntro;
		if(loc>0) queryIntro = failedQuery.substring(0, loc);
		else queryIntro = "N/A";
		
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, "The selected RDF store does not contain the necessary information to " +
			"perform the calculation.  Please select a different RDF store and try again. \n\n Failed query: \n"+queryIntro, 
			"Error", JOptionPane.ERROR_MESSAGE);
	}
	
	/**
	 * This retrieves the SELECT statement used to fill the TM hash.
	 * Executes the query in order to create the final TM hashtable.
	 */
	public void fillWithSelect() {
		
		if (DIHelper.getInstance().getLocalProp(Constants.TECH_MATURITY)!=null){
			TMhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.TECH_MATURITY);
			logger.info("TMhash was previously filled");
		}
		logger.info("Filling TMhash " + query);
		
		wrapper = new SesameJenaSelectWrapper();
		list = new ArrayList();
	
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		
		try {
			processQuery();
			DIHelper.getInstance().setLocalProperty(Constants.TECH_MATURITY, TMhash);
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets the list of names from the query.
	 * If it contains information about software lifecycle, software category, or system hardware, get the constants.
	 * Creates create or fill matrices with names if applicable.
	 */
	private void processQuery() {
		// get the bindings from it
		String [] names = wrapper.getVariables();
		
		String constant = null;
		if (names[1].contains("SystemSoftwareMLifecycle")) constant = Constants.TM_LIFECYCLE;
		else if (names[1].contains("SystemSoftwareMCategory")) constant = Constants.TM_CATEGORY;
		else if (names[1].contains("SystemHardware")) constant = Constants.TM_LIFECYCLE;
		

		if (names[0].contains("CREATE")){
			createMatrix(constant, names);
		}
		
		else if (names[0].contains("FILL")){
			fillStringMatrix(constant, names);
		}
	}
	
	/**
	 * Fills the string matrix with matrix name/type, row/column labels, and values.
	 * @param constant String
	 * @param names String[]
	 */
	public void fillStringMatrix(String constant, String[] names) {
		//This is for filling a matrix
		//first variable is FILL_MATRIX
		//second variable is matrix name
		//third variable is matrix type
		//fourth variable is row label
		//fifth variable is col label
		//sixth variable is value
		int count = 0;
		String var2 = null;
		String var3 = null;
		String var4 = null;
		String var5 = null;
		String key = null;
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.BVnext();
				try{
					var2 = Utility.getInstanceName(sjss.getVar(names[2])+"");
					var3 = Utility.getInstanceName(sjss.getVar(names[3])+"");
					var4 = Utility.getInstanceName(sjss.getVar(names[4])+"");
					if(constant.contains("_Tech_Maturity_Lifecycle"))
						var5 = (String)sjss.getVar(names[5]);
					else
						var5 = Utility.getInstanceName(sjss.getVar(names[5])+"");
					
					if(count == 0){
						String vert3uri = sjss.getVar(names[3])+"";
						String vert3type = Utility.getClassName(vert3uri);
						String vert4uri = sjss.getVar(names[4])+"";
						String vert4type = Utility.getClassName(vert4uri);
						if (!(vert3type==null) && !(vert4type==null)) key = vert3type + "/" + vert4type;
						else key = var2;
					}
					count++;
					
					//get the right matrix and arrayList
					String[][] matrix = (String[][]) TMhash.get(key+constant);
					ArrayList<String> rowLabels = (ArrayList<String>) TMhash.get(key+Constants.CALC_ROW_LABELS);
					ArrayList<String> colLabels = (ArrayList<String>) TMhash.get(key+Constants.CALC_COLUMN_LABELS);
					
					//put in the label in rowLabels and put the value in the right spot in the array
					if (!rowLabels.contains(var3)) rowLabels.add(var3);
					int rowLoc = rowLabels.indexOf(var3);
					if (!colLabels.contains(var4)) colLabels.add(var4);
					int colLoc = colLabels.indexOf(var4);
					matrix[rowLoc][colLoc] = var5;
					TMhash.put(key+constant, matrix);
					TMhash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
					TMhash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
					var2="";
					var3="";
					var4="";
					var5="";
				}
				catch(RuntimeException e)
				{
					fileLogger.info("Issue with relation: "+names[2]+" - "+var2+">>>>"+names[3]+" - "+var3+">>>>"+names[4]+" - "+var4+">>>>"+names[5]+" - "+var5);
				}
			}
		}catch (RuntimeException e) {
			logger.fatal(e);
		}
	}
	
	/**
	 * Creates keys based on parameter names in the SesameJenaSelectWrapper.
	 * Puts the keys with appropriate values in the TM hash.
	 * @param constant String
	 * @param names String[]
	 */
	private void createMatrix(String constant, String [] names){
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.BVnext();
				String key = null;
				Double vert3double=0.0;
				Double vert4double=0.0;
				try{
					if (names[1].contains("SystemSoftwareMLifecycle")) {
							key = "System/SoftwareModule";
						}
						else if (names[1].contains("SystemSoftwareMCategory")) {
							key = "System/SoftwareModule";
						}
						else if (names[1].contains("SystemHardware")) {
							key = "System/HardwareModule";
						}
						vert3double = (Double) sjss.getVar(names[3]);
						int vert3int = (int) vert3double.intValue();
						vert4double = (Double) sjss.getVar(names[4]);
						int vert4int = (int) vert4double.intValue();
						
						String[][] fullMatrix = new String[vert3int][vert4int];
						ArrayList<String> rowLabels = new ArrayList<String>();
						ArrayList<String> colLabels = new ArrayList<String>();
						
						TMhash.put(key+constant, fullMatrix);
						TMhash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
						TMhash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
				}
				catch(RuntimeException e)
				{
					fileLogger.info("Issue with relation: "+names[3]+" - "+vert3double+">>>>"+names[4]+" - "+vert4double);
				}
			}
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	
	/**
	 * Fills the TM hash.
	 */
	@Override
	public void run() {
		createView();
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}

}
