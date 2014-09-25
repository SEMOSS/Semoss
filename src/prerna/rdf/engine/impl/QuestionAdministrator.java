package prerna.rdf.engine.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Vector;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.ibm.icu.util.StringTokenizer;

public class QuestionAdministrator {
	IEngine engine;//= //(IEngine) DIHelper.getInstance().getLocalProp(selectedEngine);
	RDFFileSesameEngine insightBaseXML;// = ((AbstractEngine)engine).getInsightBaseXML();
	
	String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
	boolean reorder = true;
	
//	JButton questionModButton = (JButton) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_BUTTON);
	String questionModType = "";
	//	JComboBox<String> questionSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_MOD_SELECTOR);
	ArrayList<String> questionList = new ArrayList<String> ();
//	JComboBox<String> perspectiveSelector = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.QUESTION_PERSPECTIVE_SELECTOR);
	String selectedPerspective = "";
	public static String selectedEngine = null;

	String xmlFile = "db/" +selectedEngine + "/" + selectedEngine + "_Questions.XML";
//	
//	String questionModButtonText = questionModButton.getText();
	
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
	public static Vector<String> currentParameterOptionListArray = null;
	boolean lastQuestion;
	
	protected static final String semossURI = "http://semoss.org/ontologies/";
	protected static final String engineBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Engine";
	protected static final String perspectiveBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Perspective";
	protected static final String insightBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Insight";
	protected static final String paramBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS+"/Param";
	
	protected static final String enginePerspectiveBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Engine:Perspective";
	protected static final String perspectiveInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Perspective:Insight";
	protected static final String engineInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Engine:Insight";
	protected static final String containsBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS+"/Contains";
	protected static final String labelBaseURI = containsBaseURI + "/Label";
	protected static final String idLabelBaseURI = containsBaseURI + "/IDLabel";
	protected static final String layoutBaseURI = containsBaseURI + "/Layout";
	protected static final String sparqlBaseURI = containsBaseURI + "/SPARQL";
//	protected static final String tagBaseURI = containsBaseURI + "/Tag";
	protected static final String descriptionBaseURI = containsBaseURI + "/Description";
	
	String engineURI2 = engineBaseURI + "/" + selectedEngine;
	
	public QuestionAdministrator (IEngine engine){
		this.engine = engine;
		insightBaseXML = ((AbstractEngine)engine).getInsightBaseXML();
	}
	
	public QuestionAdministrator (IEngine engine, ArrayList<String> questionList, String selectedPerspective, String questionModType){
		this.engine = engine;
		insightBaseXML = ((AbstractEngine)engine).getInsightBaseXML();
		
		this.questionList = questionList;
		this.selectedPerspective = selectedPerspective;
		this.questionModType = questionModType;
	}
	
	public void createQuestionXMLFile(String questionXMLFile, String baseFolder){
		FileWriter fWrite = null;
		RDFXMLPrettyWriter questionXMLWriter = null;
		
		try {
			String xmlFileName = baseFolder + "/" +questionXMLFile;
			
			fWrite = new FileWriter(xmlFileName);
			questionXMLWriter  = new RDFXMLPrettyWriter(fWrite);
			//System.err.println(insightBaseXML.rc);
			insightBaseXML.rc.export(questionXMLWriter);
			
			System.err.println("Created XML Question Sheet at: " + xmlFileName);
		} catch (IOException | RDFHandlerException | RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			try{
				if(fWrite!=null){
					fWrite.close();
				} 
			} catch(IOException e){
				e.printStackTrace();
			}
			try{
				if(questionXMLWriter!=null){
					questionXMLWriter.close();
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Adds a perspective.
	 * @param perspectivePred	String for relationship between engine and perspective full URI
	 * @param perspectiveURI	String for the perspective instance full URI
	 * @param perspective		String for the perspective instance
	 */
	private void addPerspective(String perspectivePred, String perspectiveURI, String perspective){
		//TODO are engine relations added anywhere?
//		insightBaseXML.addStatement(engineURI, RDF.TYPE, insightVF.createURI(engineBaseURI));
//		insightBaseXML.addStatement(engineURI, RDFS.LABEL, insightVF.createLiteral(engineName));

		//add the perspective type uris
		insightBaseXML.addStatement(perspectiveURI, RDF.TYPE.stringValue(), perspectiveBaseURI,true);
		insightBaseXML.addStatement(perspectiveURI, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective,false);

		//creates the label on the perspective as literal
		insightBaseXML.addStatement(perspectiveURI,labelBaseURI,perspective,false);
		
		//add relationship triples for engine to perspective
		insightBaseXML.addStatement(perspectivePred, RDFS.SUBPROPERTYOF.stringValue(), enginePerspectiveBaseURI,true);
		insightBaseXML.addStatement(perspectivePred, RDFS.LABEL.stringValue(), selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR +selectedEngine + ":" + perspective,false);
		insightBaseXML.addStatement(engineURI2, perspectivePred, perspectiveURI,true);
	}
	
	private void removePerspective(String perspectivePred, String perspectiveURI, String perspective){

		//remove the perspective type uris
		insightBaseXML.removeStatement(perspectiveURI, RDF.TYPE.stringValue(), perspectiveBaseURI,true);
		insightBaseXML.removeStatement(perspectiveURI, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective,false);

		//remove the label on the perspective as literal
		insightBaseXML.removeStatement(perspectiveURI,labelBaseURI,perspective,false);
		
		//remove relationship triples for engine to perspective
		insightBaseXML.removeStatement(perspectivePred, RDFS.SUBPROPERTYOF.stringValue(), enginePerspectiveBaseURI,true);
		insightBaseXML.removeStatement(perspectivePred, RDFS.LABEL.stringValue(), selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR +selectedEngine + ":" + perspective,false);
		insightBaseXML.removeStatement(engineURI2, perspectivePred, perspectiveURI,true);
	}
	
	/**
	 * Adds question to the engine and perspective
	 * @param ePred
	 * @param qURI
	 * @param perspectiveURI
	 * @param qPred
	 */
	private void addQuestionID(String ePred, String qURI, String perspectiveURI,String perspective, String qPred){

		// add the question to the engine; if perspective change, the qURI will need to change as well
		insightBaseXML.addStatement(qURI, RDF.TYPE.stringValue(), insightBaseURI,true);
		insightBaseXML.addStatement(qURI, RDFS.LABEL.stringValue(), selectedEngine + ":"
				+ perspective + ":" + qsKey,false);

		// add the engine to the question triples
		insightBaseXML.addStatement(ePred, RDFS.SUBPROPERTYOF.stringValue(), engineInsightBaseURI,true);
		insightBaseXML.addStatement(ePred, RDFS.LABEL.stringValue(), selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey,false);	
		insightBaseXML.addStatement(engineURI2, ePred, qURI,true);

		// add question to perspective
		// perspective INSIGHT:ID id_of_question(ID)
		insightBaseXML.addStatement(qPred, RDFS.SUBPROPERTYOF.stringValue(),perspectiveInsightBaseURI,true);

		insightBaseXML.addStatement(qPred, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey,false);
		insightBaseXML.addStatement(perspectiveURI, qPred, qURI,true);
	}
	
	private void removeQuestionID(String ePred, String qURI, String perspectiveURI,String perspective, String qPred){
		// remove the question to the engine; if perspective change, the qURI will need to change as well
		insightBaseXML.removeStatement(qURI, RDF.TYPE.stringValue(), insightBaseURI,true);
		insightBaseXML.removeStatement(qURI, RDFS.LABEL.stringValue(), selectedEngine + ":"
				+ perspective + ":" + qsKey,false);

		// remove the engine to the question triples
		insightBaseXML.removeStatement(ePred, RDFS.SUBPROPERTYOF.stringValue(), engineInsightBaseURI,true);
		insightBaseXML.removeStatement(ePred, RDFS.LABEL.stringValue(), selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey,false);
		insightBaseXML.removeStatement(engineURI2, ePred, qURI,true);

		// remove question to perspective
		insightBaseXML.removeStatement(qPred, RDFS.SUBPROPERTYOF.stringValue(),perspectiveInsightBaseURI,true);
		insightBaseXML.removeStatement(qPred, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey,false);
		insightBaseXML.removeStatement(perspectiveURI, qPred, qURI,true);
	}
	
	private void addQuestionLabel(String perspectiveURI, String qURI, String qsDescr, String qPred){
		//TODO might need to add a relationship between a perspective and the label associated with the insight that perspective is related too, but i think this should just be through queries.
		insightBaseXML.addStatement(qURI, labelBaseURI, qsDescr,false);
	}
	
	private void removeQuestionLabel(String perspectiveURI, String qURI, String qsDescr, String qPred){
		insightBaseXML.removeStatement(qURI, labelBaseURI, qsDescr,false);
	}
	
	private void addQuestionIDLabel(String qURI, String qsKey){
		insightBaseXML.addStatement(qURI, idLabelBaseURI, qsKey,false);
	}
	
	private void removeQuestionIDLabel(String qURI, String qsKey){
		insightBaseXML.removeStatement(qURI, idLabelBaseURI, qsKey,false);
	}
	
	private void addQuestionSparql(String qURI, String sparql){
		insightBaseXML.addStatement(qURI,sparqlBaseURI,sparql,false);
	}
	
	private void removeQuestionSparql(String qURI, String sparql){
		insightBaseXML.removeStatement(qURI,sparqlBaseURI,sparql,false);
	}
	
	private void addQuestionLayout(String qURI, String layoutName){
		insightBaseXML.addStatement(qURI,layoutBaseURI,layoutName,false);
	}
	
	private void removeQuestionLayout(String qURI, String layoutName){
		insightBaseXML.removeStatement(qURI,layoutBaseURI,layoutName,false);
	}
	
	private void addDescription(String qURI, String descriptionPred, String description){
		insightBaseXML.addStatement(qURI, descriptionPred, description,false);
	}
	
	private void removeDescription(String qURI, String descriptionPred, String description){
		insightBaseXML.removeStatement(qURI, descriptionPred, description,false);
	}
	
	private void addQuestionParam(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties){
		while (paramKeys.hasMoreElements()) {
			String param = paramKeys.nextElement();
			String paramKey = param.substring(0, param.indexOf("-"));
			String type = param.substring(param.indexOf("-") + 1);

			String qsParamKey = paramBaseURI + "/"+ selectedEngine + ":" + perspective + ":" + qsKey + ":" + paramKey;
			
			//add parameter to the insight
			insightBaseXML.addStatement(qURI, "INSIGHT:PARAM", qsParamKey,true);
			//add a label to the param which is the label used in param panel
			insightBaseXML.addStatement(qsParamKey, "PARAM:LABEL", paramKey,false);

			// see if the param key has options (not a query) associated with it
			// usually it is of the form qsKey + _ + paramKey + _ + OPTION
			//if so, add the list of options and set the type ot be a literal
			String optionKey = qsKey + "_" + type +"_" + Constants.OPTION;
			if(parameterProperties.get(optionKey) != null) {
				String option = parameterProperties.get(optionKey);
				insightBaseXML.addStatement(qsParamKey, "PARAM:OPTION", option,false);
				insightBaseXML.addStatement(qsParamKey, "PARAM:TYPE", type, false);	
				insightBaseXML.addStatement(qsParamKey, "PARAM:HAS:DEPEND", "false", false);
				insightBaseXML.addStatement(qsParamKey, "PARAM:DEPEND", "None", false);
			}
			else {
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
					insightBaseXML.addStatement(qsParamKey, "PARAM:HAS:DEPEND", "true", false);
					while(depTokens.hasMoreElements())
					{
						String depToken = depTokens.nextToken();
						insightBaseXML.addStatement(qsParamKey, "PARAM:DEPEND", depToken, false);
					}
				}						
				else
				{
					insightBaseXML.addStatement(qsParamKey, "PARAM:HAS:DEPEND", "false", false);
					insightBaseXML.addStatement(qsParamKey, "PARAM:DEPEND", "None", false);
				}
				if(type.equals("DataObject")){
					int x = 0;
				}
				//set the type to be a uri
				insightBaseXML.addStatement(qsParamKey, "PARAM:TYPE", type,true);	
			}
			//TODO originally there was a relation between INSIGHT:TYPE, I remvoed this because you can go through param but if something breaks, check this. 
		}
	}
	
	private void removeQuestionParam(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties){
		while (paramKeys.hasMoreElements()) {
			String param = paramKeys.nextElement();
			String paramKey = param.substring(0, param.indexOf("-"));
			String type = param.substring(param.indexOf("-") + 1);

			String qsParamKey = paramBaseURI + "/"+ selectedEngine + ":" + perspective + ":" + qsKey + ":" + paramKey;
			
			//remove parameter to the insight relationship
			insightBaseXML.removeStatement(qURI, "INSIGHT:PARAM", qsParamKey,true);
			//add a label to the param which is the label used in param panel
			insightBaseXML.removeStatement(qsParamKey, "PARAM:LABEL", paramKey,false);

			// see if the param key has options (not a query) associated with it
			// usually it is of the form qsKey + _ + paramKey + _ + OPTION
			//if so, remove the list of options and set the type ot be a literal
			if(parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.OPTION) != null) {
				String option = parameterProperties.get(qsKey + "_" + paramKey +"_" + Constants.OPTION);
				insightBaseXML.removeStatement(qsParamKey, "PARAM:OPTION", option,false);
				insightBaseXML.removeStatement(qsParamKey, "PARAM:TYPE", type, false);	
				insightBaseXML.removeStatement(qsParamKey, "PARAM:HAS:DEPEND", "false", false);
				insightBaseXML.removeStatement(qsParamKey, "PARAM:DEPEND", "None", false);
			}
			else {	
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
					insightBaseXML.removeStatement(qsParamKey, "PARAM:HAS:DEPEND", "true", false);
					while(depTokens.hasMoreElements())
					{
						String depToken = depTokens.nextToken();
						insightBaseXML.removeStatement(qsParamKey, "PARAM:DEPEND", depToken, false);
					}
				}						
				else
				{
					insightBaseXML.removeStatement(qsParamKey, "PARAM:HAS:DEPEND", "false", false);
					insightBaseXML.removeStatement(qsParamKey, "PARAM:DEPEND", "None", false);
				}
		
				//remove the type to be a uri
				insightBaseXML.removeStatement(qsParamKey, "PARAM:TYPE", type,true);	
			}
		}
	}
	
	/**
	 * Fills parameterProperties with dependencies, queries, and options.
	 * The keys and values will be of one of the 3 following options:
	 * "SysP1_Data_QUERY" to "SELECT DISTINCT ...."
	 * "SysP1_NumberOfClusters_OPTION" to "1;2;3;4;5"
	 * "GQ6_Instance_DEPEND" to "Concept"
	 * @param parameterProperties
	 * @param parameterDependList
	 * @param parameterQueryList
	 * @param questionKey
	 */
	private void populateParamProps(HashMap<String, String> parameterProperties, Vector<String> parameterDependList, Vector<String> parameterQueryList, Vector<String> parameterOptionList, String questionKey){
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
		
		//add param options to the hashmap
		if(parameterOptionList!=null && parameterOptionList.size()>0){
			for(int i=0; i < parameterOptionList.size(); i++){
				String[] tmpParamOption = parameterOptionList.get(i).split("_-_");
				parameterProperties.put(questionKey+"_"+tmpParamOption[0], tmpParamOption[1]);
			}
		}
	}
	
	private void reorderQuestions(String order, String currentOrder){
		Insight in = new Insight();
		
		String localCurrentPerspective = "";
		String localCurrentSparql = "";
		HashMap<String, String> localCurrentParameterProperties = new HashMap<String, String>();
		String localCurrentQsKey = "";
		String localCurrentLayoutName = "";
		String localCurrentQsDesc = "";
		Vector<String> localCurrentParameterDependList = new Vector<String>();
		Vector<String> localCurrentParameterQueryList = new Vector<String>();
		Vector<String> localCurrentParameterOptionList = new Vector<String>();
		
		localCurrentParameterProperties.clear();
				
		int newOrderNumber = Integer.parseInt(order);
		int currentOrderNumber = Integer.parseInt(currentOrder);
		
		//get the indexed at question for current position to start manipulating order numbers for all questions under the current position
		//when moving from high # to a low #
		if(newOrderNumber < currentOrderNumber){
			for(int i = newOrderNumber; i < currentOrderNumber; i++){
				//get the value at [i] (store the old question) and replace first [i] with +1 (store the new question)
				String oldQuestion = questionList.get(i-1);
				//get the questionkey to create the appropriate triples to add and remove
				in = ((AbstractEngine)engine).getInsight2(oldQuestion).get(0);//TODO
				String localID = in.getId();
				String[] localIDSplit = localID.split(":");
				localCurrentQsKey = localIDSplit[3];
				Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine)engine).getParams(oldQuestion);//TODO
				
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
				localCurrentPerspective = localCurrentQuestionIDArray[2];
				localCurrentSparql = in.getSparql();
				localCurrentParameterProperties = new HashMap<String, String>();
				localCurrentLayoutName = in.getOutput();
				localCurrentQsDesc = in.getDescription();
				
				populateParamProps(localCurrentParameterProperties, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentParameterOptionList, localCurrentQsKey);
				
				String newQuestion = oldQuestion.replaceFirst(Integer.toString(i)+".", Integer.toString(i+1)+".");
				
				deleteQuestion(localCurrentPerspective, localCurrentQsKey, oldQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentParameterOptionList);
				addQuestion(localCurrentPerspective, localCurrentQsKey, newQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentParameterOptionList);
			}
		}
		else {
			for(int i = currentOrderNumber; i < newOrderNumber; i++){
				//get the value at [i] (store the old question) and replace first [i] with +1 (store the new question)
				String oldQuestion = questionList.get(i);

				//get the questionkey to create the appropriate triples to add and remove
				in = ((AbstractEngine)engine).getInsight2(oldQuestion).get(0);//TODO
				String localID = in.getId();
				String[] localIDSplit = localID.split(":");
				localCurrentQsKey = localIDSplit[3];
				Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine)engine).getParams(oldQuestion);//TODO
				
				if(!paramInfoVector.isEmpty()){
					for(int j = 0; j < paramInfoVector.size(); j++){
						//populates the parameterQueryList with any parameter queries users have already created
						if(!paramInfoVector.get(j).getQuery().equals("SELECT ?entity WHERE {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <@entity@>;}")){
							localCurrentParameterQueryList.add(paramInfoVector.get(j).getName() + "_QUERY_-_" + paramInfoVector.get(j).getQuery());
						}
						
						//populates the parameterDependList with any parameter dependencies userse have already created
						Vector<String> dependVars = paramInfoVector.get(j).getDependVars();
						if(!dependVars.isEmpty()){
							for(int k=0; k < dependVars.size(); k++){
								localCurrentParameterDependList.add(paramInfoVector.get(j).getName() + "_DEPEND_-_" + dependVars);
					
							}
						}
					}
				}
				String localCurrentQuestionID = in.getId();
				String[] localCurrentQuestionIDArray = localCurrentQuestionID.split(":");
				localCurrentPerspective = localCurrentQuestionIDArray[2];
				localCurrentSparql = in.getSparql();
				localCurrentParameterProperties = new HashMap<String, String>();
				localCurrentLayoutName = in.getOutput();
				localCurrentQsDesc = in.getDescription();
				
				populateParamProps(localCurrentParameterProperties, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentParameterOptionList, localCurrentQsKey);
				
				String newQuestion = oldQuestion.replaceFirst(Integer.toString(i+1)+".", Integer.toString(i)+".");
				
				deleteQuestion(localCurrentPerspective, localCurrentQsKey, oldQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentParameterOptionList);
				addQuestion(localCurrentPerspective, localCurrentQsKey, newQuestion, localCurrentSparql, localCurrentLayoutName, localCurrentQsDesc, localCurrentParameterDependList, localCurrentParameterQueryList, localCurrentParameterOptionList);
			}
		}
	}
	
	public void addQuestion(String perspective, String questionKey,
			String question, String sparql, String layout, String questionDescription, Vector<String> parameterDependList, Vector<String> parameterQueryList, Vector<String> parameterOptionList) {
		parameterProperties.clear();
		
		//take in the parameter properties and store in a hashmap; will be used when adding param properties (dependencies and param queries)
		populateParamProps(parameterProperties, parameterDependList, parameterQueryList, parameterOptionList, questionKey);
		
		// create the perspective uris
		perspectiveURI = perspectiveBaseURI+"/"+selectedEngine + ":" + perspective;
		perspectivePred = enginePerspectiveBaseURI +"/" + selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR +selectedEngine + ":" + perspective;

		qsKey = questionKey;

		qsDescr = question;
		layoutName = layout;
		description = questionDescription;

		//insight uri
		qURI = insightBaseURI + "/"+ selectedEngine + ":" + perspective + ":" + qsKey;
		
		//perspective to insight relationship
		qPred = perspectiveInsightBaseURI + "/"
				+ selectedEngine + ":" + perspective
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;
		//engine to insight relationship
		ePred = engineInsightBaseURI + "/"+ selectedEngine 
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;
		
		paramHash = Utility.getParams(sparql);		
		paramKeys = paramHash.keys();
		
		// add the question to the engine
		addPerspective(perspectivePred, perspectiveURI, perspective);
		addQuestionID(ePred, qURI, perspectiveURI,perspective,qPred);

		// add description as a property on the insight
		if (description != null){
			addDescription(qURI, descriptionBaseURI, description);
		}
		
		//add label, sparql, layout as properties
		addQuestionLabel(perspectiveURI, qURI, qsDescr, qPred);
		addQuestionIDLabel(perspectiveURI, qsKey);
		addQuestionSparql(qURI, sparql);
		addQuestionLayout(qURI, layoutName);
		
		// TODO why? isnt this done in QuestionID
//		insightBaseXML.addStatement(engineURI2, perspectivePred,
//				 perspectiveURI, true);

		addQuestionParam(paramKeys, perspective, qsKey, qURI, parameterProperties);
				
		if(questionModType.equals("Add Question")){
			String[] questionArray = question.split("\\. ", 2);
			String questionOrder = questionArray[0].trim();
			if(!selectedPerspective.equals("*NEW Perspective")){
				//if it's not the last question in the perspective; no need to reorder if question is added as the last question
				if(!(Integer.parseInt(questionOrder)==questionList.size()+1) && reorder){
					// the addQuestion method will call the reOrder method once; prevents infite loops from addQuestion to reorderQuestion
					reorder = false;
					reorderQuestions(questionOrder, questionList.size()+1+"");
				}
			}
		}
	}
	
	public void modifyQuestion(String perspective, String questionKey,
			String question, String sparql, String layout, String questionDescription, Vector<String> parameterDependList, Vector<String> parameterQueryList, Vector<String> parameterOptionList) {
		
		HashMap<String, String> currentParameterProperties = new HashMap<String, String>();
		
		currentParameterProperties.clear();
		
		// create the perspective uris
		perspectiveURI = perspectiveBaseURI+"/"+selectedEngine + ":" + perspective;
		perspectivePred = enginePerspectiveBaseURI +"/" + selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR +selectedEngine + ":" + perspective;
		
		qsKey = questionKey;

		qsDescr = question;
		layoutName = layout;
		description = questionDescription;

		paramHash = Utility.getParams(sparql);

		//insight uri
		qURI = insightBaseURI + "/"+ selectedEngine + ":" + perspective + ":" + qsKey;
		//perspective to insight relationship
		qPred = perspectiveInsightBaseURI + "/"
				+ selectedEngine + ":" + perspective
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;
		//engine to insight relationship
		ePred = engineInsightBaseURI + "/"+ selectedEngine 
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;
		
		deleteQuestion(currentPerspective, currentQuestionKey, currentQuestion, currentSparql, currentLayout, currentQuestionDescription, currentParameterDependListArray, currentParameterQueryListArray, currentParameterOptionListArray);
		addQuestion(perspective, questionKey, question, sparql, layout, questionDescription, parameterDependList, parameterQueryList, parameterOptionList);
		
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
			String question, String sparql, String layout, String questionDescription, Vector<String> parameterDependList, Vector<String> parameterQueryList, Vector<String> parameterOptionList) {
		
		parameterProperties.clear();
		
		//populates parameterProperties based on on paramProp values passed in
		populateParamProps(parameterProperties, parameterDependList, parameterQueryList, parameterOptionList, questionKey);
		
		// create the perspective uris
		perspectiveURI = perspectiveBaseURI+"/"+selectedEngine + ":" + perspective;
		perspectivePred = enginePerspectiveBaseURI +"/" + selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR +selectedEngine + ":" + perspective;
		
		// add the question
		qsKey = questionKey;
		qsDescr = question;
		layoutName = layout;
		description = questionDescription;

		paramHash = Utility.getParams(sparql);

		//insight uri
		qURI = insightBaseURI + "/"+ selectedEngine + ":" + perspective + ":" + qsKey;
		//perspective to insight relationship
		qPred = perspectiveInsightBaseURI + "/"
				+ selectedEngine + ":" + perspective
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;
		//engine to insight relationship
		ePred = engineInsightBaseURI + "/"+ selectedEngine 
				+ Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;
		
		//checks if there is only one question left in the perspective
		if(questionList.size()==1){
			lastQuestion=true;
		}
		
		//if there is only one question left, and the user is deleting it; then delete the perspective as well.
		if(lastQuestion){
			removePerspective(perspectivePred, perspectiveURI, perspective);
		}
		
		removeQuestionID(ePred, qURI, perspectiveURI,perspective,qPred);

		if (description != null){
			removeDescription(qURI, descriptionBaseURI, description);
		}
		
		removeQuestionLabel(perspectiveURI, qURI, qsDescr, qPred);
		removeQuestionIDLabel(perspectiveURI, qsKey);
		removeQuestionSparql(qURI, sparql);
		removeQuestionLayout(qURI, layoutName);

		paramKeys = paramHash.keys();

		removeQuestionParam(paramKeys, perspective, qsKey, qURI, parameterProperties);
		
		if(questionModType.equals("Delete Question")){
			String[] questionArray = question.split("\\. ", 2);
			String questionOrder = questionArray[0].trim();
			//if it's not the last question in the perspective; no need to reorder if last question is deleted
			if(!(Integer.parseInt(questionOrder)==questionList.size()) && reorder){
				reorder = false;
				reorderQuestions(questionList.size()+"", Integer.parseInt(questionOrder)+"");
			}
		}
	}
}
