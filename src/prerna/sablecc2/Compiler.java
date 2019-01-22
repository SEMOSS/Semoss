package prerna.sablecc2;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.gson.GsonUtility;

public class Compiler
{
	public static void main(String[] arguments) throws Exception
	{

		//	 String test = "{'Type':'pkql','EXPRESSION':'panel[0].config({'panelstatus':'normalized','size':{'width':'900px','height':'400px'},'position':{'top':'3px','left':'5px'}});'}";
		//	 Object yes = new Gson().fromJson(test, Object.class);
		//	 System.out.println("yes");

		//	String workingDir = System.getProperty("user.dir");
		//	String propFile = workingDir + "/RDF_Map.prop";
		//	DIHelper.getInstance().loadCoreProp(propFile);

		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);

		//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Input.smss";
		//		coreEngine = new RDBMSNativeEngine();
		//		coreEngine.setEngineName("Input");
		//		coreEngine.openDB(engineProp);
		//		DIHelper.getInstance().setLocalProperty("Input", coreEngine);
		//
		//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Proposal.smss";
		//		coreEngine = new RDBMSNativeEngine();
		//		coreEngine.setEngineName("Proposal");
		//		coreEngine.openDB(engineProp);
		//		DIHelper.getInstance().setLocalProperty("Proposal", coreEngine);

		//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		//		coreEngine = new RDBMSNativeEngine();
		//		coreEngine.setEngineName("Movie_RDBMS");
		//		coreEngine.openDB(engineProp);
		//		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\MinInput.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("MinInput");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("MinInput", coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\MinProposal.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("MinProposal");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("MinProposal", coreEngine);

		String expression = ""

				//+ "j:<code>import prerna.util.Console;import java.util.Hashtable;System.out.println(\"Crabby Patty !! \"); Hashtable myHash = new Hashtable();String data = null; data.toString();<code>;"
				//    												+ "network.connect();network.disconnect();"
				//+ "pig(s=[a,b,v], s=[t,t,p], <c>System.out.println(\"Hello World\"); 3+ 5;<c>);" 
				//+ "pig(s=[a,b,c], filter=[(s == ['ab', 'cd','ef']), (a > [1])], join=[(a inner.join b)], <c>System.out.println(\"Hello World\");<c>).as([select_a]) |  monkey(s=[select_a], props=[type=\"reduce\"]).as([group_b]) | donkey(a,select_a, group_b);" // | trial(s=[a,b,c],props=[name=\"samiksha\"]);"
				//+ "sum((2*abc)+product(5,2));"
				//+ "sum(<c>System.out.println(1);<c>).try(c=['hello']);"


				//													+ "plan = Database(\"Input\") "
				//													+ "| Select(INPUTCSV,INPUTCSV__Alias_1,INPUTCSV__Client_ID,INPUTCSV__FieldName,INPUTCSV__FormName,INPUTCSV__Scenario,INPUTCSV__Type_1,INPUTCSV__Value_1,INPUTCSV__Version) "
				//													+ "| Iterate() "
				//													+ "| LoadClient(assignment = ['Alias_1'], value = ['Value_1']);"
				//													+ "proposals = Database(\"Proposal\") "
				//													+ "| Select(PROPOSALCSV,PROPOSALCSV__Alias_1,PROPOSALCSV__Client_ID,PROPOSALCSV__FieldName,PROPOSALCSV__FormName,PROPOSALCSV__ProposalName,PROPOSALCSV__Type_1,PROPOSALCSV__Value_1,PROPOSALCSV__Version) "
				//													+ "| Iterate();"
				//													+ "executedPlan = RunPlan(PLANNER = [plan]);"
				//													+ "planData = RunTaxPlan(PLANNER = [executedPlan], PROPOSALS = [proposals]);"
				//													+ "planData = RunTaxPlan(PLANNER = [plan], PROPOSALS = [proposals]); "



													+ "plan = Database(\"MinInput\") "
													+ "| Select(INPUTCSV,INPUTCSV__Hashcode,INPUTCSV__Client_ID,INPUTCSV__FieldName,INPUTCSV__FormName,INPUTCSV__Scenario,INPUTCSV__Type_1,INPUTCSV__Value_1,INPUTCSV__Version) "
													+ "| Iterate() "
													+ "| LoadClient(assignment = ['Hashcode'], value = ['Value_1'], type = ['Type_1']);"
													+ "proposals = Database(\"MinProposal\") | "
													+ "Select(PROPOSALCSV,PROPOSALCSV__Hashcode,PROPOSALCSV__Client_ID,PROPOSALCSV__FieldName,PROPOSALCSV__FormName,PROPOSALCSV__ProposalName,PROPOSALCSV__Type_1,PROPOSALCSV__Value_1,PROPOSALCSV__Version) "
													+ "| Iterate(); "
													+ "executedPlan = RunPlan(PLANNER = [plan]); "
													+ "planData = RunTaxPlan(PLANNER = [executedPlan], PROPOSALS = [proposals]); "
													+ "retData = TaxRetrieveValue(PLANNER=[planData], "
													+ "key=[\"aBE\","
													+ "\"aBF\","
													+ "\"aBG\","
													+ "\"aBH\""
													+ "]);"
													+ "Iterate(store=[retData]) | "
													+ "AddFormat(formatName = [\"d2\"], type = [\"keyvalue\"]) | "
													+ "Export(target = [\"abcd\"], formatName = ['d2']) | Collect(1000);"




//													+ "plan = Database(\"MinInput\") "
//													+ "| Select(INPUTCSV,INPUTCSV__Alias_1,INPUTCSV__Client_ID,INPUTCSV__FieldName,INPUTCSV__FormName,INPUTCSV__Scenario,INPUTCSV__Type_1,INPUTCSV__Value_1,INPUTCSV__Version, INPUTCSV__Hashcode) "
//+ "| Select(UPDATEDINPUTCSV ,  UPDATEDINPUTCSV__Alias_1 ,  UPDATEDINPUTCSV__Client_ID ,  UPDATEDINPUTCSV__FieldName ,  UPDATEDINPUTCSV__FormName ,  UPDATEDINPUTCSV__Scenario ,  UPDATEDINPUTCSV__Type_1 ,  UPDATEDINPUTCSV__Value_1 ,  UPDATEDINPUTCSV__Version) "
//													+ "| Iterate() "
//													+ "| LoadClient(assignment = ['Hashcode'], value = ['Value_1']);"
//													+ "proposals = Database(\"MinProposal\") "
//													+ "| Select(PROPOSALCSV,PROPOSALCSV__Alias_1,PROPOSALCSV__Client_ID,PROPOSALCSV__FieldName,PROPOSALCSV__FormName,PROPOSALCSV__ProposalName,PROPOSALCSV__Type_1,PROPOSALCSV__Value_1,PROPOSALCSV__Version, PROPOSALCSV__Hashcode) "
//													+ "| Iterate();"
//													+ "executedPlan = RunPlan(PLANNER = [plan]);"
//													+ "planData = RunTaxPlan(PLANNER = [executedPlan], PROPOSALS = [proposals]); "
//													+ "UpdatePlan(PLANNER = [plan], store=[retData], pksls=["
//													+ "\"ATAX_REFORM_SCENARIOS__SELECTED_LIMITATION_FOR_163J = 1000;\" "
//													+ ", \"AFORECASTING_PERCENTAGE__11__TOTAL_INCOME = 37322000;\" "
//													+ ", \"A1120_PG_1_MAPPING__24__EMPLOYEE_BENEFIT_PROGRAMS = -45673134;\" "
//													+ ", \"ASCH_C_MAPPING__TOTAL_DIVIDENDS__ADD_LINES_1_THROUGH_17__ENTER_HERE_AND_ON_PAGE_1__LINE_4_ = 8797652721;\" "
//													+ "]);"

//													+ "Database(Movie_RDBMS) | Select(Title, Title__Movie_Budget, Title__Revenue_Domestic, Title__Revenue_International, Studio) | Filter((Title__Movie_Budget > 45), (45 < Title__Revenue_International), (Title__Revenue_Domestic > Title__Revenue_International)) | Join((Title inner.join Studio)) | Import(); "
//													+ "Frame() | Select(Studio, Sum(Movie_Budget)) | Group(Studio) | Iterate();"
//													
//													+ "5;"
//													+ "Job('job1') | AddFormat(formatName = ['d1'], type = ['table']) | "
//													+ "AddOptions(optionsName = ['o1'], label = ['Studio'], value = ['Sum_MovieBudget']) | "
//													+ "Export(target = ['bar'], formatName = ['d1'], optionsName = ['o1']) | Collect(10);"
//													+ " ( Sum(Movie_Budget) / 10000000 + (2*5/3) ) ^ 7;"
//													+ "x = 5; y = 7; if( (x*60 > y*8.9), Median(10,3,4,5,6,6), 20.5);"
//													+ "Database(\"test2\") | "
//													+ "Select(UPDATEDINPUTCSV, UPDATEDINPUTCSV__Alias_1, UPDATEDINPUTCSV__Client_ID, UPDATEDINPUTCSV__FieldName, UPDATEDINPUTCSV__FormName, UPDATEDINPUTCSV__Scenario, UPDATEDINPUTCSV__Type_1, UPDATEDINPUTCSV__Value_1, UPDATEDINPUTCSV__Version) | "
//													+ "Iterate() | "
//													+ "(plan = LoadClient(assignment = ['Alias_1'], value = ['Value_1'], separator = ['__']) ); "
//													+ "retData = RunPlan(PLANNER = [plan]); "
//													+ "UpdatePlan(PLANNER = [plan], store=[retData], pksls=["
//																		+ "\"ATAX_REFORM_SCENARIOS__SELECTED_LIMITATION_FOR_163J = 1000;\" "
//																		+ ", \"AFORECASTING_PERCENTAGE__11__TOTAL_INCOME = 37322000;\" "
//																		+ ", \"A1120_PG_1_MAPPING__24__EMPLOYEE_BENEFIT_PROGRAMS = -45673134;\" "
//																		+ ", \"ASCH_C_MAPPING__TOTAL_DIVIDENDS__ADD_LINES_1_THROUGH_17__ENTER_HERE_AND_ON_PAGE_1__LINE_4_ = 8797652721;\" "
//																+ "]) | "
//													+ "newRet = RetrieveValue(store=['retData'], key=['ASCH_M_3_OTHER_ITEMS_MAPPING_LAB_TESTING_PERMANENT_DIFFERENCE','AFORECASTING_PERCENTAGE_20_DEPRECIATION_FORM_4562_MANUAL']); "
//													+ "Iterate(store=[newRet]) | AddFormat(formatName = ['d1'], type = ['keyvalue']) | "
//													+ "Export(target = ['1120_Pg1_Form'], formatName = ['d1']) | Collect(10000); "

//													+ "Job('job2') | AddFormat(formatName = ['d1'], type = ['keyvalue']) | "
//													+ "AddOptions(optionsName = ['o1'], label = ['Studio'], value = ['Sum_MovieBudget']) | "
//													+ "Export(target = ['bar'], formatName = ['d1'], optionsName = ['o1']) | Collect(10000);"

//													+ "Job('job2') | AddFormat(formatName = ['d1'], type = ['keyvalue']) | "
//													+ "Export(target = ['1120_Pg1_Form'], formatName = ['d1']) | Collect(10000);"

//													+ "a = 10; b = 5; c = b-a;"
//													+ "a = 10; b = (a); Sum(a, b, -6); if( (b-a > 10), 6, 8);"

//													+ "Database(Movie_RDBMS) | Select(Title, Title__Movie_Budget, Title__Revenue_Domestic, Title__Revenue_International, Studio) | Filter((Title__Movie_Budget > 45), (45 < Title__Revenue_International), (Title__Revenue_Domestic > Title__Revenue_International)) | Join((Title inner.join Studio)) | Import(); "
//													+ "a = Sum(Movie_Budget); 
//													+ "1b = (-6); "
//													+ "Sum( Sum(Movie_Budget), 1b, 7);"
//													+ "Database(Clean_Sch_J) | Select(CLEAN_SCHJ_IMPACTRESULTCSV, CLEAN_SCHJ_IMPACTRESULTCSV__FieldName, CLEAN_SCHJ_IMPACTRESULTCSV__FormName, CLEAN_SCHJ_IMPACTRESULTCSV__Type_1, CLEAN_SCHJ_IMPACTRESULTCSV__Value_1) | Iterate() | LoadClient(assignment=['FormName', 'FieldName'], value=['Value_1'], separator=['__']);"
//													+ "Database(\"clean_1120_page_mapping\") | Select(CLEAN_1120_PAGE_MAPPINGCSV, CLEAN_1120_PAGE_MAPPINGCSV__Client_ID, CLEAN_1120_PAGE_MAPPINGCSV__FieldName, CLEAN_1120_PAGE_MAPPINGCSV__FormName, CLEAN_1120_PAGE_MAPPINGCSV__Scenario, CLEAN_1120_PAGE_MAPPINGCSV__Type_1, CLEAN_1120_PAGE_MAPPINGCSV__Value_1, CLEAN_1120_PAGE_MAPPINGCSV__Version ) | Iterate() | LoadClient(assignment = ['FormName', 'FieldName'], value = ['Value_1'], separator = ['__']);"


//													+"test = MapStore();; StoreValue(store=[test], key=['thiskey'], value=[5000]);RetrieveValue(store=[test], key=['thiskey']);"
//													+ "if( ( RetrieveValue(store=['test'], key=['thiskey']) > 10 ) , if((2*(3+Sum(Movie_Budget)) > 9), 0, 1) , 6);"

//													+ "Database(Movie_RDBMS) | Select(Title, Title__Movie_Budget, Title__Revenue_Domestic, Title__Revenue_International, Studio) | Filter((Title__Movie_Budget > 45), (45 < Title__Revenue_International), (Title__Revenue_Domestic > Title__Revenue_International)) | Join((Title inner.join Studio)) | Import(); "
//													+ "a = Sum(Movie_Budget);; "
//													+ "if( ( (Sum(Revenue_Domestic) + a) > 10000000), if((6 > 5), 10, 5), 0);"

													//+ "if(((a + 2 *5)==10), if(b, a*2, if(c, k*9+2*3+h, 0)), if(c, b*2, 0));"
													//+ "sum((2*abc)+5*2);sum(s=[hello, 'hello world', sum(hello, world)], k=[(name != 'gh'), (somethingelse == 5)])|product(s=[demo]) | product2(s=[demo]); sum(2 + 3); sum2(2 + 3); sum3(a,b,c,d) | sum4(a,b,c); sum7(s=[a,b,c, sum6(a,b,c)]); name = 'pk';"
													//+ "(3 + 4, [c:bp, c:ab]);"
													//+ "col.import([c:col1,c:col2], ([c:col1, inner.join, c:col2])) . [[\"a\",\"b\"][2,3]];"
													//+ ";" //; , [[2,1.0][\"a hello world would work absolutely fine too __ ab\",\"b\"]]);"
													//    												+ "col.add(c:newCol, (4 - c:Capability) + c:Activity); "
													//    												+ "api:Movie_DB.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Title__Title,  inner.join , c:Studio__Studio])); "
													//    												+ "col.add(c:test, api:UpdatedRDBMSMovies.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Title__Title,  right.outer.join , c:Studio__Title_FK])));"
													//    												+ "col.add(c:test, api:UpdatedRDBMSMovies.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Studio__Title_FK,  right.outer.join , c:Title__Title])));"
													//    												+ "data.import(api:MovieDatabase.query([c:Title, c:Studio], (c:Studio =[\"WB\",\"Fox\"]), ([c:Title,  inner.join , c:Studio])));"
													//    												+ "data.import(api:Movie_DB.query([c:Title, c:Director], ([c:Title,  inner.join , c:Director])), ([c:Title, inner.join, c:Title]));"
													//    												+ "data.import(api:Movie_Results.query([c:Title, c:Producer], ([c:Title,  inner.join , c:Producer])), ([c:Title, inner.join, c:Title]));"
													//    												+ "data.import(api:Movie_Results.query([c:Title, c:Year], ([c:Title,  inner.join , c:Year])), ([c:Title, inner.join, c:Title]));"
													//    												+ "data.import(api:Documents\\something.csv.query([c:Title, c:Year]), ([c:Title, inner.join, c:Title]));"
													//    												+ "data.import(api:csvFile.query([c:Title, c:Studio, c:Year], {'file':'C:\\Users\\rluthar\\Documents\\Movie Results.csv'}));"
													//    												+ "m:Sum([c:Year]);"
													//    												+ "data.open(\"Movie_RDBMS\", \"1\");"
													//    												+ "data.import(api:csvFile.query([c:column1, c:column3], {'file':'C:\\Users\\rluthar\\Documents\\test2.csv'}), ([c:column1, outer.join, c:column1]));"
													//    												+ "col.add(c:newCol, (c:MovieBudget + c:Revenue-Domestic));"
													//    												+ "panel[0].setbuilder('this');"
													//    												+ "panel[0].clone(1);"
													//													+ "col.add(c:newCol, (c:MovieBudget - c:Revenue-Domestic); "
													//    												
													//    												+ "data.import(api:csvFile.query([c:Title, c:Nominated, c:Studio], (c:Title = ['12 Years a Slave'], c:Nominated = ['Y']), {'file':'C:\\Users\\bisutton\\Desktop\\Movie Data.csv'}));"
													//    												+ "data.import(api:csvFile.query([c:Title, c:Actor], {'file':'C:\\Users\\bisutton\\Desktop\\Best Actor.csv'}), ([c:Title, inner.join, c:Title]));"

													//    												+"panel[0].comment[0].add( 'test',svgMain,{'sort':{'descending':'Title'},'key':'value','key':'value'}, test1);"
													//    												+"panel[0].comment[0].edit('test',svgMain,{'sort':{'descending':'Title'},'key':'value','key':'value'}, test1);"
													//    												+"panel[0].comment[0].remove();"
													//    												+"panel[0].tools({'color':['#F1433F','#660066','#F7E967','#B3CF8B','#EB99FF','#4D6529','#B3110D','#0066CC','#A49508','#6699FF'],'stackToggle':'group-data','colorName':'Semoss','backgroundColor':'#FFFFFF'});"

													//													+"panel[0].lookandfeel({'color':{'thisbar':'red'},'key':'value','key':'value'});"
													//													+"panel[0].tools({'sort':{'descending':'Title'},'key':'value','key':'value'});"
													//													+"panel[0].config({'position':{'x':11},'show':'true'});"
													//    												
													//    												+ "panel[1].comment({'sort':{'descending':'Title'},'key':'value','key':'value'});"
													//    												+"panel[\"123\"].comment(\"this looks super important\", group0, coordinate, \"a\");"
													//    												+"col.filter(c:Studio =[\"WB\",\"Fox\",\"Lionsgate\"]) ;"
													//    												+"col.unfilter(c:Studio);"
													//    												+ "col.add(c:test, api:Movie_DB.query([c:Title__Title, c:Title__MovieBudget, c:Studio__Studio], (c:Studio__Studio =[\"WB\",\"Fox\"]), ([c:Studio__Studio,  right.outer.join , c:Title__Title])));"
													//+ "api:TAP_CORE.query([c:Title__title, c:Title__movie_budget], (c:Title__Title =[\"a\",\"b\"], c:Title__Title > [\"c\", \"d\"]), ([c:Title__Title,  inner.join , c:Studio__Studio] ,[c:Title__Title,  outer.join , c:Studio__Studio_FK]) ); "    												//+ "(m:Sum([(2 * 3) * (c:Capability * c:Activity) + c:Activity], [c:bp, c:ab]));"
													//+ "3 + (m:Sum([(2 * 3) * (c:Capability * c:Activity) + c:Activity], [c:bp, c:ab]));"
													// m:Sum(X) >= m:Sum(Capability)
													//+ "[a,b,c,d,e,f,e];"
													// can I use the calculation once again in something else ?
													// I almost need to break everytime I see a math function
													// but how do I tell groovy not to use it anymore ?
													// i.e. the value is there so just take that
													// I bet I need to replace the equation so that I can do it
													// in reality I only need to reduce when I need to < - that is profound prabhu
													// Ok.. I bet what I am saying is I need to reduce only when I see the pattern closing and then I need to replace it in the expression




													//    												+"(12 + (4 - 8)) * (15 / 5) + 5;"
													//+ "v:abc = (c:col2 * (2 *4)); "
													/*+ "set:(c:newCol, (2 * 3) + 4); "
   													+ "code:{System.out.println('yo');};"
    												+ "pivot:(c:col1, c:col2);"
   													+ "join:(c:newColumn, c:existingcol, c:existingcol2);"
   													+ "del:(c:colToRemove);"
   													+ "rm:(c:colToRemove, c:anotherColToRemove);"*/
   													//+ "jc"
   													//+"database.concepts(Movie_RDBMS)"	
   													// new InputStreamReader(System.in), 1024)));

			// Parse the input.
			+ "";

		// Apply the translation.
		Insight in = new Insight();
		PixelRunner runner = in.runPixel(expression);
		Gson gson = GsonUtility.getDefaultGson();
		System.out.println(gson.toJson(runner.getResults()));
	}
}