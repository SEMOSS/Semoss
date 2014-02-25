package prerna.poi.main;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import prerna.util.Constants;

public class NLPReader extends AbstractFileReader {
	public Hashtable<String,Object> temp = new Hashtable<String,Object>(); 
	public Hashtable<String,Object> temp2 = new Hashtable<String,Object>();
	public Hashtable<String,Object> temp3 = new Hashtable<String,Object>();
	
	
	public void importFileWithOutConnection(String engineName, String fileNames, String customBase, String customMap, String owlFile) throws Exception 
	{	
		System.out.println("HERE2");
		String[] files = prepareReader(fileNames, customBase, owlFile);
		openEngineWithoutConnection(engineName);
		 ArrayList <TripleWrapper> Triples = new ArrayList();
		
		if(!customMap.equals("")) 
		{
			openProp(customMap);
		}
		//if user selected a map, load just as before--using the prop file to discover Excel->URI translation
			System.out.println("MMC Begin");
			WebsiteNLP docReader = new WebsiteNLP();
			Triples = docReader.MasterRead(files);
			System.out.println("MMC End");
		
		System.out.println("HERE1");
		createNLPrelationships(Triples);
	//	createRelationship("subject", "predicate", "Jessica", "ran", "predofsub", temp);
	//	sc.commit();
	//	createRelationship("subject", "predicate", "Tom", "ran", "predofsub", temp);
	//	createRelationship("predicate", "object", "ran", "home", "objofpred", temp);
		System.out.println("HERE0");
		createBaseRelations();
		closeDB();
		
	}
	
	public void createNLPrelationships( ArrayList<TripleWrapper> Triples) throws Exception{
		double thick1 = 3;
		double thick2 = 9;
		
		for(int i = 0; i<Triples.size(); i++){
			temp.put("occurance", Triples.get(i).getObj1num());
			temp2.put("occurance", Triples.get(i).getPrednum());
			temp3.put("occurance", Triples.get(i).getObj2num());
			createRelationship("subject", "predicate", Triples.get(i).getObj1(), Triples.get(i).getPred(),"subjectofpredicate", temp);
			createRelationship("predicate","object", Triples.get(i).getPred(), Triples.get(i).getObj2(),"predicateofobject", temp);
			createRelationship("object", "subject", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"objectofsubject", temp2);
			 
			addNodeProperties("subject",Triples.get(i).getObj1(),temp);
			addNodeProperties("predicate",Triples.get(i).getPred(),temp2);
		    addNodeProperties("object",Triples.get(i).getObj2(),temp3);
			
		    temp.remove("occurance");
			temp2.remove("occurance");
			temp3.remove("occurance");
			
			createRelationship("subject", "subjectexpanded", Triples.get(i).getObj1(), Triples.get(i).getObj1exp(),"expandedofsubject", temp);
			createRelationship("predicate", "predicateexpanded", Triples.get(i).getPred(), Triples.get(i).getPredexp(),"expandedofpredicate", temp);
			createRelationship("object", "objectexpanded", Triples.get(i).getObj2(), Triples.get(i).getObj2exp(),"expandedofobject", temp);
			
			createRelationship("subject", "articlenum", Triples.get(i).getObj1(), Triples.get(i).getArticleNum(),"articleofsubject", temp);
			createRelationship("predicate", "articlenum", Triples.get(i).getPred(), Triples.get(i).getArticleNum(),"articleofpredicate", temp);
			createRelationship("object", "articlenum", Triples.get(i).getObj2(), Triples.get(i).getArticleNum(),"articleofobject", temp);
			
		//	createRelationship("subject", "sobjectcount", Triples.get(i).getObj1(), Triples.get(i).getObj1num(),"numofsubject", temp);
		//	createRelationship("predicate", "predicatecount", Triples.get(i).getPred(), Triples.get(i).getPrednum(),"numofpredicate", temp);
		//	createRelationship("object", "objectcount", Triples.get(i).getObj2(), Triples.get(i).getObj2num(),"numofobject", temp);
			
		}
	}
	
	

	private void importFile(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void setRdfMap(Hashtable<String, String> propHash) {
		// TODO Auto-generated method stub
		
	}
}

