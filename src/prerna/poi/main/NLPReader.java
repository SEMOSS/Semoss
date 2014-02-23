package prerna.poi.main;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import prerna.util.Constants;

public class NLPReader extends AbstractFileReader {
	public Hashtable<String,Object> temp = new Hashtable<String,Object>(); 
	
	
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
		
		for(int i = 0; i<Triples.size(); i++){
			createRelationship("subject", "predicate", Triples.get(i).getObj1(), Triples.get(i).getPred(),"subjectofpredicate", temp);
			createRelationship("predicate","object", Triples.get(i).getPred(), Triples.get(i).getObj2(),"predicateofobject", temp);
			createRelationship("object", "subject", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"objectofsubject", temp);
			
			createRelationship("subject", "subjectexpanded", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"subjectexpanded", temp);
			createRelationship("predicate", "predicateexpanded", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"predicateexpanded", temp);
			createRelationship("object", "objectexpanded", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"objectexpanded", temp);
			
			createRelationship("subject", "articlenum", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"articleofsubject", temp);
			createRelationship("predicate", "articlenum", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"articleofpredicate", temp);
			createRelationship("object", "articlenum", Triples.get(i).getObj2(), Triples.get(i).getObj1(),"articleofobject", temp);
			
		}
	}
	
	

	private void importFile(String fileName) {
		// TODO Auto-generated method stub
		
	}

	public void setRdfMap(Hashtable<String, String> propHash) {
		// TODO Auto-generated method stub
		
	}
}

