package prerna.rdf.engine.impl;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComboBox;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.ibm.icu.util.StringTokenizer;

public class QuestionAdministrator{
	public static String selectedEngine = null;
	String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
	String engineURI2 = "database:" + selectedEngine;
	boolean reorder = true;
	AbstractEngine insightBaseXML;
	
	JButton questionModButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_BUTTON);
	JComboBox<String> questionSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_SELECTOR);
	JComboBox<String> questionDBSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_DB_SELECTOR);
	JComboBox<String> perspectiveSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_SELECTOR);
	String selectedDB = (String) questionDBSelector.getSelectedItem();
	String xmlFile = "db/" +selectedDB + "/" + selectedDB + "_Questions.XML";

	String questionModButtonText = questionModButton.getText();
	
	//the following are variables used to in storing the triples for the questions
	HashMap<String, String> parameterProperties = new HashMap<String, String>();
	String perspectiveURI;
	String perspectivePred;
	String qsKey;
	String qsDescr;
	String layoutName;
	String description;
	Hashtable paramHash;
	String qURI;
	String qPred;
	String ePred;
	String descriptionPred;
	Enumeration<String> paramKeys;
	
	//following variables will hold the current values before being modified by user
	public static String currentPerspective = null;
	public static String currentQuestionKey = null;
	public static String currentQuestion = null;
	public static String currentLayout = null;
	public static String currentSparql = null;
	public static String currentQuestionDescription = null;
	public static Vector<String> currentParameterDependListArray = null;
	public static Vector<String> currentParameterQueryListArray = null;
	boolean lastQuestion;
	
	public QuestionAdministrator() {
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(selectedEngine);
		insightBaseXML = ((AbstractEngine)engine).getInsightBaseXML();
	}
	
	private void addPerspective(String perspectivePred, String perspectiveURI, String perspective){
		//database:VA_MainDB
		insightBaseXML.addStatement(engineURI2, perspectivePred,
				perspectiveURI, true);
		//VA_MainDB:PERSPECTIVE:Generic-Perspective
		insightBaseXML.addStatement(perspectiveURI, Constants.PERSPECTIVE + ":"
				+ Constants.LABEL, perspective, false);
	}
	
	private void removePerspective(String perspectivePred, String perspectiveURI, String perspective){
		//database:VA_MainDB
		insightBaseXML.removeStatement(engineURI2, perspectivePred,
				perspectiveURI, true);
		//VA_MainDB:PERSPECTIVE:Generic-Perspective
		insightBaseXML.removeStatement(perspectiveURI, Constants.PERSPECTIVE + ":"
				+ Constants.LABEL, perspective, false);
	}
	
	private void addQuestionID(String ePred, String qURI, String perspectiveURI, String qPred){
		// add the question to the engine; if perspective change, the qURI will need to change as well
		//<ID xmlns="ENGINE:" rdf:resource="VA_MainDB:Generic-Perspective:GQ1"/>
		insightBaseXML.addStatement(engineURI2, ePred, qURI, true);
		// // <ID rdf:resource="VA_MainDB:Activity-Perspective:ActP2"/>
		insightBaseXML.addStatement(perspectiveURI, qPred, qURI, true);
	}
	
	private void removeQuestionID(String ePred, String qURI, String perspectiveURI, String qPred){
		//remove the question from the engine
		//<ID xmlns="ENGINE:" rdf:resource="VA_MainDB:Generic-Perspective:GQ1"/>
		insightBaseXML.removeStatement(engineURI2, ePred, qURI, true);
		// <ID rdf:resource="VA_MainDB:Activity-Perspective:ActP2"/>
		insightBaseXML.removeStatement(perspectiveURI, qPred, qURI, true);
	}
	
	private void addQuestionLabel(String perspectiveURI, String qURI, String qsDescr, String qPred){
		// perspective INSIGHT:INSIGHT label_of_question
		// <INSIGHT xmlns="INSIGHT:">Explore an instance of a selected node type</INSIGHT>
		insightBaseXML.addStatement(perspectiveURI, Constants.INSIGHT + ":"
				+ Constants.INSIGHT, qsDescr, false);
		
		// perspective INSIGHT:ID id_of_question(ID)
		//	<ID rdf:resource="VA_MainDB:Generic-Perspective:GQ1"/>
		 insightBaseXML.addStatement(perspectiveURI, qPred, qURI, true);
		 
		// ID insight:label label
		//	<LABEL xmlns="INSIGHT:">Explore an instance of a selected node type</LABEL>
		insightBaseXML.addStatement(qURI, Constants.INSIGHT + ":"
				+ Constants.LABEL, qsDescr, false);
	}
	
	private void removeQuestionLabel(String perspectiveURI, String qURI, String qsDescr, String qPred){
		// perspective INSIGHT:INSIGHT label_of_question
		// <INSIGHT xmlns="INSIGHT:">Explore an instance of a selected node type</INSIGHT>
		insightBaseXML.removeStatement(perspectiveURI, Constants.INSIGHT + ":"
				+ Constants.INSIGHT, qsDescr, false);
		
		// perspective INSIGHT:ID id_of_question(ID)
		//	<ID rdf:resource="VA_MainDB:Generic-Perspective:GQ1"/>
		insightBaseXML.removeStatement(perspectiveURI, qPred, qURI, true);
		 
		// ID insight:label label
		//	<LABEL xmlns="INSIGHT:">Explore an instance of a selected node type</LABEL>
		insightBaseXML.removeStatement(qURI, Constants.INSIGHT + ":"
				+ Constants.LABEL, qsDescr, false);
	}
	
	private void addQuestionSparql(String qURI, String sparql){
		// ID insight:sparql sparql
		insightBaseXML.addStatement(qURI, Constants.INSIGHT + ":"
				+ Constants.SPARQL, sparql, false);
	}
	
	private void removeQuestionSparql(String qURI, String sparql){
		// ID insight:sparql sparql
		insightBaseXML.removeStatement(qURI, Constants.INSIGHT + ":"
				+ Constants.SPARQL, sparql, false);
	}
	
	private void addQuestionLayout(String qURI, String layoutName){
		// ID insight:output output
		insightBaseXML.addStatement(qURI, Constants.INSIGHT + ":"
				+ Constants.OUTPUT, layoutName, false);
	}
	
	private void removeQuestionLayout(String qURI, String layoutName){
		// ID insight:output output
		insightBaseXML.removeStatement(qURI, Constants.INSIGHT + ":"
				+ Constants.OUTPUT, layoutName, false);
	}
	
	private void addDescription(String qURI, String descriptionPred, String description){
		// insight INSIGHT:DESCRIPTION description
		insightBaseXML.addStatement(qURI, descriptionPred, description,
				false);
	}
	
	private void removeDescription(String qURI, String descriptionPred, String description){
		// insight INSIGHT:DESCRIPTION description
		insightBaseXML.removeStatement(qURI, descriptionPred, description,
				false);
	}
	
	private void addQuestionParam(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties){
		while (paramKeys.hasMoreElements()) {
			String param = paramKeys.nextElement();
			String paramKey = param.substring(0, param.indexOf("-"));
			String type = param.substring(param.indexOf("-") + 1);

			String qsParamKey = selectedEngine + ":" + perspective + ":"
					+ qsKey + ":" + paramKey;

			// add this parameter to the query
			insightBaseXML
					.addStatement(qURI, "INSIGHT:PARAM", qsParamKey, true);
			insightBaseXML.addStatement(qsParamKey, "PARAM:TYPE", type, false);
			insightBaseXML.addStatement(qsParamKey, "INSIGHT:PARAM:LABEL",
					paramKey, false);

			// see if the param key has a query associated with it
			// usually it is of the form qsKey + _ + paramKey + _ + Query
			String result = DIHelper.getInstance().getProperty(
					"TYPE" + "_" + Constants.QUERY);

			if(parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.QUERY) != null)
				// record this
				// qskey_paramKey - Entity:Query - result
				result = parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.QUERY);
				insightBaseXML.addStatement(qsParamKey, "PARAM:QUERY", result, false);
			
			// see if there is dependency
			// dependency is of the form qsKey + _ + paramKey + _ + Depend
			if(parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.DEPEND) != null)
			{
				// record this
				// qsKey_paramkey  - qsKey:Depends - result
				result = parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.DEPEND);
				StringTokenizer depTokens = new StringTokenizer(result, ";");
				insightBaseXML.addStatement(qsParamKey, "HAS:PARAM:DEPEND", "true", false);
				while(depTokens.hasMoreElements())
				{
					String depToken = depTokens.nextToken();
					insightBaseXML.addStatement(qsParamKey, "PARAM:DEPEND", depToken, false);
				}
			}						
			else
			{
				insightBaseXML.addStatement(qsParamKey, "HAS:PARAM:DEPEND", "false", false);
				insightBaseXML.addStatement(qsParamKey, "PARAM:DEPEND", "None", false);
			}

			// add it to insight base
			// engineName has perspective
			if (type.contains(":")) {
				String typeURI = type;
				// type INSIGHT:TYPE id_of_the_question
				insightBaseXML.addStatement(typeURI, Constants.INSIGHT + ":"
						+ Constants.TYPE, qURI, true);
			}
		}
	}
	
	private void removeQuestionParam(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties){
		while (paramKeys.hasMoreElements()) {
			String param = paramKeys.nextElement();
			String paramKey = param.substring(0, param.indexOf("-"));
			String type = param.substring(param.indexOf("-") + 1);

			String qsParamKey = selectedEngine + ":" + perspective + ":"
					+ qsKey + ":" + paramKey;

			// add this parameter to the query
			insightBaseXML
					.removeStatement(qURI, "INSIGHT:PARAM", qsParamKey, true);
			insightBaseXML.removeStatement(qsParamKey, "PARAM:TYPE", type, false);
			insightBaseXML.removeStatement(qsParamKey, "INSIGHT:PARAM:LABEL",
					paramKey, false);

			// see if the param key has a query associated with it
			// usually it is of the form qsKey + _ + paramKey + _ + Query
			String result = DIHelper.getInstance().getProperty(
					"TYPE" + "_" + Constants.QUERY);

			if(parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.QUERY) != null)
				// record this
				// qskey_paramKey - Entity:Query - result
				result = parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.QUERY);
				insightBaseXML.removeStatement(qsParamKey, "PARAM:QUERY", result, false);
			
			// see if there is dependency
			// dependency is of the form qsKey + _ + paramKey + _ + Depend
			if(parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.DEPEND) != null)
			{
				// record this
				// qsKey_paramkey  - qsKey:Depends - result
				result = parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.DEPEND);
				StringTokenizer depTokens = new StringTokenizer(result, ";");
				insightBaseXML.removeStatement(qsParamKey, "HAS:PARAM:DEPEND", "true", false);
				while(depTokens.hasMoreElements())
				{
					String depToken = depTokens.nextToken();
					insightBaseXML.removeStatement(qsParamKey, "PARAM:DEPEND", depToken, false);
				}
			}						
			else
			{
				insightBaseXML.removeStatement(qsParamKey, "HAS:PARAM:DEPEND", "false", false);
				insightBaseXML.removeStatement(qsParamKey, "PARAM:DEPEND", "None", false);
			}

			// add it to insight base
			// engineName has perspective
			if (type.contains(":")) {
				String typeURI = type;
				// type INSIGHT:TYPE id_of_the_question
				insightBaseXML.removeStatement(typeURI, Constants.INSIGHT + ":"
						+ Constants.TYPE, qURI, true);
			}
		}
	}
	
	private void populateParamProps(HashMap<String, String> parameterProperties, Vector<String> parameterDependList, Vector<String> parameterQueryList, String questionKey){
		//add dependencies to the hashmap
		if(parameterDependList!=null && parameterDependList.size()>0){
			for(int i=0; i < parameterDependList.size(); i++){
				String[] tmpParamDepend = parameterDependList.get(i).split("_-_");
				parameterProperties.put(questionKey+"_"+tmpParamDepend[0], tmpParamDepend[1]);
			}
		}
		
		//add param queries to the hashmap
		if(parameterQueryList!=null && parameterQueryList.size()>0){
			for(int i=0; i < parameterQueryList.size(); i++){
				String[] tmpParamQuery = parameterQueryList.get(i).split("_-_");
				parameterProperties.put(questionKey+"_"+tmpParamQuery[0], tmpParamQuery[1]);
			}
		}
	}
	
	private void reorderQuestions(String order, String currentOrder){
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(selectedEngine);
		Insight in = new Insight();
		
		String localCurrentPerspective = "";
		String localCurrentSparql = "";
		HashMap<String, String> localCurrentParameterProperties = new HashMap<String, String>();
		String localCurrentQsKey = "";
		String localCurrentLayoutName = "";
		String localCurrentQsDesc = "";
		Vector<String> localCurrentParameterDependList = new Vector<String>();
		Vector<String> localCurrentParameterQueryList = new Vector<String>();
		
		localCurrentParameterProperties.clear();
				
		int newOrderNumber = Integer.parseInt(order);
		int currentOrderNumber = Integer.parseInt(currentOrder);
		
		//get the indexed at question for current position to start manipulating order numbers for all questions under the current position
		//when moving from high # to a low #
		if(newOrderNumber < currentOrderNumber){
			for(int i = newOrderNumber; i < currentOrderNumber; i++){
				//get the value at [i] (store the old question) and replace first [i] with +1 (store the new question)
				String oldQuestion = questionSelector.getItemAt(i-1);
				//get the questionkey to create the appropriate triples to add and remove
				in = ((AbstractEngine)engine).getInsight2(oldQuestion).get(0);
				String localID = in.getId();
				String[] localIDSplit = localID.split(":");
				localCurrentQsKey = localIDSplit[2];
				Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine)engine).getParams(oldQuestion);
				
				if(!paramInfoVector.isEmpty()){
					for(int j = 0; j < paramInfoVector.size(); j++){
						//populates the parameterQueryList with any parameter queries users have already created
						if(!paramInfoVector.get(j).getQuery().equals("SELECT ?entity WHERE {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <@entity@>;}")){
							localCurrentParameterQueryList.add(paramInfoVector.get(j).getName() + "_QUERY_-_" + paramInfoVector.get(j).getQuery());
						}
						
						//populates the parameterDependList with any parameter dependencies userse have already created
						Vector<String> dependVars = paramInfoVector.get(j).getDependVars();
						if(!dependVars.isEmpty()){
							String dependVarsConcat = "";
							for(int k=0; k<dependVars.size(); k++){
								dependVarsConcat += dependVars.get(k);
							}
							localCurrentParameterDependList.add(paramInfoVector.get(j).getName() + "_DEPEND_-_" + dependVarsConcat);
						}
					}
				}
				String localCurrentQuestionID = in.getId();
				String[] localCurrentQuestionIDArray = localCurrentQuestionID.split(":");
				localCurrentPerspective = localCurrentQuestionIDArray[1];
				localCurrentSparql = in.getSparql();
				localCurrentParameterProperties = new HashMap<String, String>();
				localCurrentLayoutName = in.getOutput();
				localCurrentQsDesc = in.getDescription();
				
				populateParamProps(localCurrentParameterProperties, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentQsKey);
				
				String newQuestion = oldQuestion.replaceFirst(Integer.toString(i)+".", Integer.toString(i+1)+".");
				
				deleteQuestion(localCurrentPerspective, localCurrentQsKey, oldQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList);
				addQuestion(localCurrentPerspective, localCurrentQsKey, newQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList);
			}
		}
		else {
			for(int i = currentOrderNumber; i < newOrderNumber; i++){
				//get the value at [i] (store the old question) and replace first [i] with +1 (store the new question)
				String oldQuestion = questionSelector.getItemAt(i);

				//get the questionkey to create the appropriate triples to add and remove
				in = ((AbstractEngine)engine).getInsight2(oldQuestion).get(0);
				String localID = in.getId();
				String[] localIDSplit = localID.split(":");
				localCurrentQsKey = localIDSplit[2];
				Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine)engine).getParams(oldQuestion);
				
				if(!paramInfoVector.isEmpty()){
					for(int j = 0; j < paramInfoVector.size(); j++){
						//populates the parameterQueryList with any parameter queries users have already created
						if(!paramInfoVector.get(j).getQuery().equals("SELECT ?entity WHERE {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <@entity@>;}")){
							localCurrentParameterQueryList.add(paramInfoVector.get(j).getName() + "_QUERY_-_" + paramInfoVector.get(j).getQuery());
						}
						
						//populates the parameterDependList with any parameter dependencies userse have already created
						Vector<String> dependVars = paramInfoVector.get(j).getDependVars();
						if(!dependVars.isEmpty()){
							String dependVarsConcat = "";
							for(int k=0; k<dependVars.size(); k++){
								dependVarsConcat += dependVars.get(k);
							}
							localCurrentParameterDependList.add(paramInfoVector.get(j).getName() + "_DEPEND_-_" + dependVarsConcat);
						}
					}
				}
				String localCurrentQuestionID = in.getId();
				String[] localCurrentQuestionIDArray = localCurrentQuestionID.split(":");
				localCurrentPerspective = localCurrentQuestionIDArray[1];
				localCurrentSparql = in.getSparql();
				localCurrentParameterProperties = new HashMap<String, String>();
				localCurrentLayoutName = in.getOutput();
				localCurrentQsDesc = in.getDescription();
				
				populateParamProps(localCurrentParameterProperties, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentQsKey);
				
				String newQuestion = oldQuestion.replaceFirst(Integer.toString(i+1)+".", Integer.toString(i)+".");
				
				deleteQuestion(localCurrentPerspective, localCurrentQsKey, oldQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList);
				addQuestion(localCurrentPerspective, localCurrentQsKey, newQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList);
			}
		}
	}
	
	public void addQuestion(String perspective, String questionKey,
			String question, String sparql, String layout, String questionDescription, Vector<String> parameterDependList, Vector<String> parameterQueryList) {
		parameterProperties.clear();
		
		//take in the parameter properties and store in a hashmap; will be used when adding param properties (dependencies and param queries)
		populateParamProps(parameterProperties, parameterDependList, parameterQueryList, questionKey);
		
		// add the perspective
		perspectiveURI = selectedEngine + ":" + Constants.PERSPECTIVE
				+ ":" + perspective;
		perspectivePred = Constants.PERSPECTIVE + ":"
				+ Constants.PERSPECTIVE;

		qsKey = questionKey;

		qsDescr = question;
		layoutName = layout;
		description = questionDescription;

		paramHash = Utility.getParams(sparql);

		qURI = selectedEngine + ":" + perspective + ":" + qsKey;
		qPred = Constants.PERSPECTIVE + ":" + Constants.ID;
		ePred = Constants.ENGINE + ":" + Constants.ID;
		descriptionPred = Constants.INSIGHT + ":" + Constants.DESCR;
		
		paramKeys = paramHash.keys();
		
		// add the question to the engine
		addPerspective(perspectivePred, perspectiveURI, perspective);
		addQuestionID(ePred, qURI, perspectiveURI,qPred);

		// add description to insight
		// insight INSIGHT:DESCRIPTION description
		if (description != null){
			addDescription(qURI, descriptionPred, description);
		}

		addQuestionLabel(perspectiveURI, qURI, qsDescr, qPred);
		addQuestionSparql(qURI, sparql);
		addQuestionLayout(qURI, layoutName);
		
		// engine perspective:perspective perspectiveURI
		insightBaseXML.addStatement(engineURI2, perspectivePred,
				 perspectiveURI, true);

		addQuestionParam(paramKeys, perspective, qsKey, qURI, parameterProperties);
		
		if(questionModButtonText.equals("Add Question")){
			String[] questionArray = question.split("\\. ", 2);
			String questionOrder = questionArray[0].trim();
			if(perspectiveSelector.getSelectedItem()!="*NEW Perspective"){
				int questionCount = questionSelector.getItemCount();
				//if it's not the last question in the perspective; no need to reorder if question is added as the last question
				if(!(Integer.parseInt(questionOrder)==questionSelector.getItemCount()+1) && reorder){
					// the addQuestion method will call the reOrder method once; prevents infite loops from addQuestion to reorderQuestion
					reorder = false;
					reorderQuestions(questionOrder, questionSelector.getItemCount()+1+"");
				}
			}
		}
	}

	public void modifyQuestion(String perspective, String questionKey,
			String question, String sparql, String layout, String questionDescription, Vector<String> parameterDependList, Vector<String> parameterQueryList) {
		
		HashMap<String, String> currentParameterProperties = new HashMap<String, String>();
		
		currentParameterProperties.clear();
		
		// add the perspective
		perspectiveURI = selectedEngine + ":" + Constants.PERSPECTIVE
				+ ":" + perspective;
		perspectivePred = Constants.PERSPECTIVE + ":"
				+ Constants.PERSPECTIVE;

		qsKey = questionKey;

		qsDescr = question;
		layoutName = layout;
		description = questionDescription;

		paramHash = Utility.getParams(sparql);

		qURI = selectedEngine + ":" + perspective + ":" + qsKey;
		qPred = Constants.PERSPECTIVE + ":" + Constants.ID;
		ePred = Constants.ENGINE + ":" + Constants.ID;
		descriptionPred = Constants.INSIGHT + ":" + Constants.DESCR;
		
		deleteQuestion(currentPerspective, currentQuestionKey, currentQuestion, currentSparql, currentLayout, currentQuestionDescription, currentParameterDependListArray, currentParameterQueryListArray);
		addQuestion(perspective, questionKey, question, sparql, layout, questionDescription, parameterDependList, parameterQueryList);
		
		//check if user has modified the question label
		if(!currentQuestion.equals(question)){
			//check if the order number changed; if it did then add the new query and loops through all subsequent questions to re-order them
			String[] currentQuestionArray = currentQuestion.split("\\. ", 2);
			String currentOrder = currentQuestionArray[0].trim();
			
			String[] questionArray = question.split("\\. ", 2);
			String order = questionArray[0].trim();
			
			if(!currentOrder.equals(order) && reorder){
				reorder = false;
				reorderQuestions(order, currentOrder);
			}
		}
	}

	public void deleteQuestion(String perspective, String questionKey,
			String question, String sparql, String layout, String questionDescription, Vector<String> parameterDependList, Vector<String> parameterQueryList) {
		
		parameterProperties.clear();
		
		//populates parameterProperties based on on paramProp values passed in
		populateParamProps(parameterProperties, parameterDependList, parameterQueryList, questionKey);
		
		// perspective
		perspectiveURI = selectedEngine + ":" + Constants.PERSPECTIVE
				+ ":" + perspective;

		perspectivePred = Constants.PERSPECTIVE + ":"
				+ Constants.PERSPECTIVE;
		
		// add the question
		qsKey = questionKey;
		qsDescr = question;
		layoutName = layout;
		description = questionDescription;

		paramHash = Utility.getParams(sparql);

		qURI = selectedEngine + ":" + perspective + ":" + qsKey;
		qPred = Constants.PERSPECTIVE + ":" + Constants.ID;
		ePred = Constants.ENGINE + ":" + Constants.ID;
		descriptionPred = Constants.INSIGHT + ":" + Constants.DESCR;
		
		//checks if there is only one question left in the perspective
		if(questionSelector.getItemCount()==1){
			lastQuestion=true;
		}
		
		//if there is only one question left, and the user is deleting it; then delete the perspective as well.
		if(lastQuestion){
			removePerspective(perspectivePred, perspectiveURI, perspective);
		}
		
		removeQuestionID(ePred, qURI, perspectiveURI,qPred);

		if (description != null){
			removeDescription(qURI, descriptionPred, description);
		}
		
		removeQuestionLabel(perspectiveURI, qURI, qsDescr, qPred);
		removeQuestionSparql(qURI, sparql);
		removeQuestionLayout(qURI, layoutName);

		paramKeys = paramHash.keys();

		removeQuestionParam(paramKeys, perspective, qsKey, qURI, parameterProperties);
		
		if(questionModButtonText.equals("Delete Question")){
			String[] questionArray = question.split("\\. ", 2);
			String questionOrder = questionArray[0].trim();
			//if it's not the last question in the perspective; no need to reorder if last question is deleted
			if(!(Integer.parseInt(questionOrder)==questionSelector.getItemCount()) && reorder){
				reorder = false;
				reorderQuestions(questionSelector.getItemCount()+"", Integer.parseInt(questionOrder)+"");
			}
		}
	}
}
