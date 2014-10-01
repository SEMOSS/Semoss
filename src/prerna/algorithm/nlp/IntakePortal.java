package prerna.algorithm.nlp;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TypedDependency;
import edu.ucla.sspace.matrix.ArrayMatrix;






public class IntakePortal {
	int MasterSize = 144;
	static StanfordNLPfuncts NLP = new StanfordNLPfuncts();
	
	
	public static void main(String[] args) throws InvalidFormatException, IOException {
		IntakePortal test = new IntakePortal();
		//	System.out.println("Start");
	//	GroupSimilarityTest();
	//	System.out.println("End");	
		
	//	System.out.println("Start");
	//	NonLearnedMapping();
	//	System.out.println("End");
	
	//	System.out.println("Start");
	//	WordNetMapping();
	//	System.out.println("End");
		
	/*	System.out.println("Start1");
		Scanner s = new Scanner(System.in);
		String A = "SystemTasker";
		String B = "ApplicationTask";
		while(true){
		System.out.println("SCORE ~ " +DBRecSimScore(A, B) +"~"+ A+"~"+B);
		System.out.println("End");
		A = s.nextLine();
		B = s.nextLine();
		}
		*/
		
     		System.out.println("Start");
     		test.DBRecSimTester();
			System.out.println("End");
		
		
	}
	
	@SuppressWarnings("resource")
	private void DBRecSimTester() throws InvalidFormatException, IOException{
		//write to text file
		FileWriter outFile;
		 outFile = new FileWriter("test.txt");
         PrintWriter out = new PrintWriter(outFile);
         
		//done writting to text file
		
		int rows = 3;
		double score2 = 0;
		IntakePortal test = new IntakePortal();
		ArrayList <String> SchemaElements1 = new ArrayList<String>();
		ArrayList <String> SchemaElements2 = new ArrayList<String>();
		ExcelReadRelationship2(SchemaElements1, SchemaElements2, rows);
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		for(int i = 0; i<SchemaElements1.size(); i++){
			for(int j=0; j<SchemaElements2.size(); j++){
			score2 = test.DBRecSimScore(SchemaElements1.get(i), SchemaElements2.get(j));
			System.out.println("SCORE "+"~"+ score2 + "~" + SchemaElements1.get(i) + "~"+SchemaElements2.get(j));
			out.println("SCORE "+"~"+ score2 + "~" + SchemaElements1.get(i).toString() + "~"+SchemaElements2.get(j).toString());
			}
			
		}
		out.close();
		
	}
	public static double[][] DBRecSimScore(ArrayList<String> A, ArrayList<String> B) {
		double[][] retScores = new double[A.size()][B.size()];
		for(int a=0;a<A.size();a++) {
			for(int b=0;b<B.size();b++) {
				retScores[a][b] = DBRecSimScore(A.get(a),B.get(b));
			}
		}
		return retScores;
	}
	public static double DBRecSimScore(String A, String B) {
		double MasterScore = 0;
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		ArrayList<String> PhraseA = new ArrayList<String>();
		ArrayList<String> PhraseB = new ArrayList<String>();
		ArrayList<String> LPhraseA = new ArrayList<String>();
		ArrayList<String> LPhraseB = new ArrayList<String>();
		ArrayList<Double> Apriority = new ArrayList<Double>();
		ArrayList<Double> Bpriority = new ArrayList<Double>();
		ArrayList<Boolean> AIsWord = new ArrayList<Boolean>();
		ArrayList<Boolean> BIsWord = new ArrayList<Boolean>();
		System.out.println("InputA "+A);
		System.out.println("InputB "+B);
		//break_up camel case and underscore division
		A = termseperator(A);
		B = termseperator(B);
		//System.out.println("DividedA "+A);
		//System.out.println("DividedB "+B);
		//break single string into arraylist based on space
		PhraseA = word_Break(A);
		PhraseB = word_Break(B);
		//gives priority ranking to arraylist
		Apriority = NLP_prioritization(A, PhraseA);
		Bpriority = NLP_prioritization(B, PhraseB);
		System.out.println("PriorA "+ Apriority);
		System.out.println("PriorB "+ Bpriority);
		//Lemmatize Terms
		LPhraseA = 	PreProcessLematize(PhraseA);
		LPhraseB =  PreProcessLematize(PhraseB);
		//System.out.println("lemmatizeA "+ LPhraseA);
		//System.out.println("lemmatizeB "+ LPhraseB);
		//Determine Word or Not Word
		AIsWord = IsWNWord(PhraseA);
		BIsWord = IsWNWord(PhraseB);
		System.out.println("WordA " + AIsWord);
		System.out.println("WordB " + BIsWord);
		//Create Similarity Score
		
		MasterScore = WordNetScorer.WN_NLP_Scorer(A, B, LPhraseA, LPhraseB, Apriority, Bpriority, AIsWord, BIsWord);
	    
	    return MasterScore;
		
		
	}

	private static ArrayList<Boolean> IsWNWord(ArrayList<String> phraseA) {
		WS4Jwrapper WNSimFunc = new WS4Jwrapper();
		double score = 0;
		ArrayList<Boolean> IsWord = new ArrayList<Boolean>();
		for(int i =0; i<phraseA.size(); i++){
			score = WNSimFunc.run2(phraseA.get(i).trim(), "entity");
			if(score == 0)
				IsWord.add(false);
			else
				IsWord.add(true);
		}
		return IsWord;
	}

	private static ArrayList<String> word_Break(String a) {
		ArrayList<String> words1 = new ArrayList<String>();
        
    	StringTokenizer word1tok = new StringTokenizer(a);
    	
    	while(word1tok.hasMoreElements()){
    		words1.add(word1tok.nextElement().toString().trim());
    	}
    	return words1;
		}

	private static ArrayList<Double> NLP_prioritization(String a, ArrayList<String> phraseA) {
		
		List<TypedDependency> tdl = new ArrayList<TypedDependency>();
		Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
		ArrayList <TaggedWord> TaggedWords = new ArrayList<TaggedWord>();
		ArrayList <Double> TermP = new ArrayList<Double>();
		//find NLP dependencies
		tdl = NLP.CreateDepList(a, tdl, TaggedWords); //create dependencies
		//organize by dependencies in Hash table
		NLP.setHash(tdl, nodeHash);
	//	System.out.println(nodeHash);
		//NN: Noun Compound Modifier is the Primary relationship. If it exists we know high priority to last work
		// and same low priority to other preceding words
		TermP = NLP.TermPrioritization(a, nodeHash, tdl, phraseA);
	//	System.out.println("prioritization " +TermP);
		//lemmatize all terms
		
		//determine if word/not word
		return TermP;
		
	}

	private static String termseperator(String Term) {
		Term = splitCamelCase(Term);
		Term = Term.replace('_', ' ');
		return Term;
		
	}

	public static double[][] WordNetMappingFunction(ArrayList <String> SchemaElements1, ArrayList <String> SchemaElements2) throws InvalidFormatException, IOException {
		
		double[][] SchemaMatrix = new double[SchemaElements1.size()][SchemaElements2.size()];
		PreProcess(SchemaElements1);
		PreProcess(SchemaElements2);
		
		System.out.println("Post Processing SchemaElements1 "+ SchemaElements1);
		System.out.println("Post Processing SchemaElements2 "+ SchemaElements2);
		//create wordsimilarity scores
		
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		for(int i = 0; i<SchemaElements1.size(); i++){
			for(int j=0; j<SchemaElements2.size(); j++)
			SchemaMatrix[i][j] = WordNetScorer.dynamicRunner(SchemaElements1.get(i), SchemaElements2.get(j));
			
		}
		return SchemaMatrix;
	}
	
	private static void WordNetMapping() throws InvalidFormatException, IOException {
		//initialization terms
		double score = 0;
		ArrayList <String> SchemaElements1 = new ArrayList<String>();
		ArrayList <String> SchemaElements2 = new ArrayList<String>();
		ArrayList <Double> MatrixRow = new ArrayList<Double>();
		//Create output Excel sheet
		HSSFWorkbook workbookout = new HSSFWorkbook();
		//setup output loader sheet
		//establishloader2(workbookout);
		LevenshteinDistance LevAnalyzer = new LevenshteinDistance();
		NeedlemanWunsch NWAnalyzer = new NeedlemanWunsch();
		//gather data from Relationship Excel pass HashTable for relationship and ArrayList for Items to be mapped
		int rows = 9;
		ExcelReadRelationship2(SchemaElements1, SchemaElements2, rows); //fills schemaelements1 and schemaelements2
		
		System.out.println("SchemaElements1 "+ SchemaElements1);
		System.out.println("SchemaElements2 "+ SchemaElements2);

		double[][] SchemaMatrix = new double[SchemaElements1.size()][SchemaElements2.size()];
	
		//testing of WordNetMappingFunction
		//	System.out.println("START TEST");
	//	SchemaMatrix = WordNetMappingFunction(SchemaElements1, SchemaElements2);
		
	//	for(int i = 0; i<SchemaMatrix.length; i++){
	//		for(int j=0; j<SchemaMatrix[0].length; j++)
	//		System.out.print(SchemaMatrix[i][j]+"	");
	//		System.out.println();
	//	}
		
	//	System.out.println("END TEST");
		
		
		//divide camel case and _ word seperation
		PreProcess(SchemaElements1);
		PreProcess(SchemaElements2);
		
		//lemmatize terms
		PreProcessLematize(SchemaElements1);
		PreProcessLematize(SchemaElements2);
		
		System.out.println("Post Processing SchemaElements1 "+ SchemaElements1);
		System.out.println("Post Processing SchemaElements2 "+ SchemaElements2);
		//create wordsimilarity scores
		
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		for(int i = 0; i<SchemaElements1.size(); i++){
			for(int j=0; j<SchemaElements2.size(); j++)
			WordNetScorer.dynamicRunner(SchemaElements1.get(i), SchemaElements2.get(j));
		}
		
		
		
	}
	
	

	private static ArrayList<String> PreProcessLematize(ArrayList<String> schemaElements) {
		//this breaks up the camel case
				for(int i = 0; i<schemaElements.size(); i++){
					schemaElements.set(i, NLP.lemmatize(schemaElements.get(i)).toString().trim());
				}
				return schemaElements;
		
	}

	static String splitCamelCase(String s) {
		   return s.replaceAll(
		      String.format("%s|%s|%s",
		         "(?<=[A-Z])(?=[A-Z][a-z])",
		         "(?<=[^A-Z])(?=[A-Z])",
		         "(?<=[A-Za-z])(?=[^A-Za-z])"
		      ),
		      " "
		   );
		}


	private static void PreProcess(ArrayList<String> terms) {

		//this breaks up the camel case
		for(int i = 0; i<terms.size(); i++){
			terms.set(i, splitCamelCase(terms.get(i)) );
			terms.set(i, terms.get(i).replace('_', ' '));
		}
		
	}


	private static void ExcelReadRelationship2(ArrayList<String> schemaElements1, ArrayList<String> schemaElements2, int rows) throws InvalidFormatException, IOException {
		// TODO Auto-generated method stub
		System.out.println("ReadRelationship");
		int NumRows = rows;	//4148
		InputStream inp = new FileInputStream("C:\\Users\\sabidi\\workspace\\TermNormalization\\SchemaMapping.xlsx");
		Workbook wb = WorkbookFactory.create(inp);
		Sheet sheet = wb.getSheet("RunValues");//get the sheet where you want to pull information
		Row row = sheet.getRow(1);
		//Store each schema
		System.out.println("Getting Schema");
		for(int i = 0; i<65000; i++){
		row = sheet.getRow(i);
		if(row == null)
			break;
		
		System.out.println(i);
		if(row.getCell(0) != null)
		schemaElements1.add(row.getCell(0).toString());
		if(row.getCell(1) != null)
		schemaElements2.add(row.getCell(1).toString());
		}
		
	}


	private static void NonLearnedMapping() throws InvalidFormatException, IOException {
				//initialization terms
				ArrayList <String> OrphanList = new ArrayList<String>();
				LetterPairSimilarity LPSmatcher = new LetterPairSimilarity();
				Hashtable<String, ArrayList<String>> RelationHash = new Hashtable<String, ArrayList<String>>();
				//Create output Excel sheet
				HSSFWorkbook workbookout = new HSSFWorkbook();
				//setup output loader sheet
				establishloader2(workbookout);
				LevenshteinDistance LevAnalyzer = new LevenshteinDistance();
				NeedlemanWunsch NWAnalyzer = new NeedlemanWunsch();
				//gather data from Relationship Excel pass HashTable for relationship and ArrayList for Items to be mapped
				ExcelReadRelationship(RelationHash, OrphanList); //needs to be modified to get C terms "unmapped unique terms"
				
				System.out.println("RelationHash "+ RelationHash);
				System.out.println("OrphanedList "+ OrphanList);
				
				 int[] LC = new int[3];
				 //Machine Learning - Training Levinstein Distance function using Expectation-Maximization on known sets
				 // An implimentation of Mikhail Bilenko and Raymond J. Mooney's Adaptive Duplicate Detection Using Learnable String Similarity Measures
				 
				 LC = ExpMaxOnRelation(RelationHash, LC);
			
				//Create enumerator for the different groups you will analyze
				ArrayList <String> Group = new ArrayList<String>();
				Enumeration<String> enumKey = RelationHash.keys();
				int i = 0;
				int UMcount = 0;
				int count = 0;
				double SimilarityScore = 0;
				double SimilarityScoreLV = 0;
				double SimilarityScoreNW = 0;
					while(enumKey.hasMoreElements()) {
						String key = enumKey.nextElement();
						System.out.println("Current Group being cross analyzed "+ key);
						Group = RelationHash.get(key);
						SimilarityScore = 0;
						SimilarityScoreLV = 0;
						SimilarityScoreNW = 0;
						//correlation matrix for each group is created: B(i) to B(j) (similarity) analysis
							for(int m = 0; m<Group.size();m++){
								for(int j = 0; j<Group.size();j++){
								//	LetterPairSimilarity
								//just now	SimilarityScore = LPSmatcher.compareStrings(Group.get(m), Group.get(j)); //old similarity method
								//  Levinshtein
								//	SimilarityScore = LevAnalyzer.computeLevenshteinDistance(Group.get(m), Group.get(j), 1, 1, 1);
								//Creates Group>B(i), Group>B(j), B(i)>B(j) (Similarity)   
								// open this line	System.out.println("Excel Rows ~ 1 ~ "+ key+ "~"+ Group.get(m)+ "~"+ Group.get(j)+ "~"+ SimilarityScore+ "~"+count);
								//just now	fillloader(key,Group.get(m),Group.get(j),SimilarityScore, workbookout, count);
									count++;
								}
							}
						//conduct ItemB(i) > (UnMapped) ItemC(i) (Similarity) analysis
						
							for(int m = 0; m<OrphanList.size();m++){
								System.out.println("Orphan group "+ m +" of "+ OrphanList.size());
								for(int j = 0; j<Group.size();j++){
									//compare item(j) of the current group to item(m) of the unmapped 
								//	SimilarityScoreLV = LevAnalyzer.computeLevenshteinSimilarity(Group.get(j), OrphanList.get(m), 1, 1, 1);
									//System.out.println("Excel Rows ~ 3 ~ "+ key+ "~"+ Group.get(j)+ "~"+ OrphanList.get(m)+ "~"+ SimilarityScore+ "~"+count);
									SimilarityScore = LPSmatcher.compareStrings(Group.get(j), OrphanList.get(m));
								//	SimilarityScoreNW = NWAnalyzer.NeddleWunschscorer(Group.get(j), OrphanList.get(m));
									if(SimilarityScore > .50){
										SimilarityScoreLV = LevAnalyzer.computeLevenshteinSimilarity(Group.get(j), OrphanList.get(m), 1, 1, 1);
										SimilarityScoreNW = NWAnalyzer.NeddleWunschscorer(Group.get(j), OrphanList.get(m));
										//fillUnMappedLoader(key,Group.get(j), OrphanList.get(m), SimilarityScore, workbookout, UMcount); //for use for single metric
										fillUnMappedLoaderMultiMeasure(key,Group.get(j), OrphanList.get(m), SimilarityScore,SimilarityScoreLV , SimilarityScoreNW, 0, workbookout, UMcount);
										System.out.println("Excel Rows ~ 2 ~ "+ key+ "~"+ Group.get(j)+ "~"+ OrphanList.get(m)+ "~"+ SimilarityScore+ "~"+ SimilarityScoreLV+ "~"+ SimilarityScoreNW+"~"+count);
										UMcount++;
										//FillLoader Group, ItemB, ItemC, Similarity
									}
								}
							}
						
						i++;
						}
					
					
					//export workbook to output excel
					try {
					    FileOutputStream out = 
					            new FileOutputStream(new File("C:\\Users\\sabidi\\workspace\\TermNormalization\\ICDtermSetRelationship.xls"));
					    workbookout.write(out);
					    out.close();
					    System.out.println("Excel written successfully..");
					     
					} catch (FileNotFoundException e) {
					    e.printStackTrace();
					} catch (IOException e) {
					    e.printStackTrace();
					}
		
	}


	private static void fillUnMappedLoader(String key, String TermA, String Orphan, double similarityScore, HSSFWorkbook workbookout, int i) {
		i=i+1; //to pass loadersheet heading
		HSSFSheet sheet;
		Row row;
		sheet = workbookout.getSheet("TermA-AutoMappedTerm");
		if(i == 1)
			row = sheet.getRow(i);
			else
				row = sheet.createRow(i);
		//Just for Printing Purposes
		row.createCell(0);
		row.createCell(1);
		row.createCell(2);
		row.createCell(3);
		row.getCell(0).setCellValue(key);
		row.getCell(1).setCellValue(TermA);
		row.getCell(2).setCellValue(Orphan);
		row.getCell(3).setCellValue(similarityScore);
		
	}
	
	private static void fillUnMappedLoaderMultiMeasure(String key, String TermA, String Orphan, double similarityScore1, double similarityScore2, double similarityScore3, double similarityScore4, HSSFWorkbook workbookout, int i) {
		i=i+1; //to pass loadersheet heading
		HSSFSheet sheet;
		Row row;
		sheet = workbookout.getSheet("TermA-AutoMappedTerm");
		if(i == 1)
			row = sheet.getRow(i);
			else
				row = sheet.createRow(i);
		//Just for Printing Purposes
		row.createCell(0);
		row.createCell(1);
		row.createCell(2);
		row.createCell(3);
		row.createCell(4);
		row.createCell(5);
		row.createCell(6);
		row.getCell(0).setCellValue(key);
		row.getCell(1).setCellValue(TermA);
		row.getCell(2).setCellValue(Orphan);
		row.getCell(3).setCellValue(similarityScore1);
		row.getCell(4).setCellValue(similarityScore2);
		row.getCell(5).setCellValue(similarityScore3);
		row.getCell(6).setCellValue(similarityScore4);
		
	}


	private static void GroupSimilarityTest() throws InvalidFormatException, IOException{
		//initialization terms
		ArrayMatrix CCM = new ArrayMatrix(114,114);
		ArrayList <String> OrphanList = new ArrayList<String>();
		LetterPairSimilarity LPSmatcher = new LetterPairSimilarity();
		Hashtable<String, ArrayList<String>> RelationHash = new Hashtable<String, ArrayList<String>>();
		//Create output Excel sheet
		HSSFWorkbook workbookout = new HSSFWorkbook();
		//setup output loader sheet
		establishloader(workbookout);
		LevenshteinDistance LevAnalyzer = new LevenshteinDistance();
		
		//gather data from Relationship Excel
		ExcelReadRelationship(RelationHash, OrphanList); //needs to be modified to get C terms "unmapped unique terms"
		System.out.println("RelationHash "+ RelationHash);
		
		
		//Create enumerator for the different groups you will analyze
		ArrayList <String> Group = new ArrayList<String>();
		Enumeration<String> enumKey = RelationHash.keys();
			int i = 0;
			int count = 0;
			double SimilarityScore = 0;
			while(enumKey.hasMoreElements()) {
				String key = enumKey.nextElement();
				System.out.println("Current Group "+ key);
				Group = RelationHash.get(key);
				
				SimilarityScore = 0;
					for(int m = 0; m<Group.size();m++){
						for(int j = 0; j<Group.size();j++){
							SimilarityScore = LPSmatcher.compareStrings(Group.get(m), Group.get(j)); //old similarity method
							SimilarityScore = LevAnalyzer.computeLevenshteinDistance(Group.get(m), Group.get(j), 1, 1, 1);
						    System.out.println("Excel Rows ~ "+ key+ "~"+ Group.get(m)+ "~"+ Group.get(j)+ "~"+ SimilarityScore+ "~"+count);
							fillloader(key,Group.get(m),Group.get(j),SimilarityScore, workbookout, count);
							count++;
						}
					}
				
				i++;
				}
			
			
			//export workbook to output excel
			try {
			    FileOutputStream out = 
			            new FileOutputStream(new File("C:\\Users\\sabidi\\workspace\\TermNormalization\\ICDtermSetRelationship.xls"));
			    workbookout.write(out);
			    out.close();
			    System.out.println("Excel written successfully..");
			     
			} catch (FileNotFoundException e) {
			    e.printStackTrace();
			} catch (IOException e) {
			    e.printStackTrace();
			}
	}




	private static int[] ExpMaxOnRelation(
			Hashtable<String, ArrayList<String>> relationHash, int[] LC) {
		// TODO Auto-generated method stub
		LC[0] = 1;
		LC[1] = 1;
		LC[2] = 1;
		return LC;
		
	}


	private static void fillloader(String key, String termA, String termB, double similarityScore, HSSFWorkbook workbookout, int i) {
		//System.out.println("Excel Rows ~"+ key+ "~"+ termA+ "~"+ termB+ "~"+ similarityScore+ "~" + i);
		i=i+1; //to pass loadersheet heading
		HSSFSheet sheet;
		Row row;
		sheet = workbookout.getSheet("Group-TermA");
		if(i == 1)
			row = sheet.getRow(i);
			else
				row = sheet.createRow(i);
		row.createCell(1);
		row.createCell(2);
		row.getCell(1).setCellValue(key);
		row.getCell(2).setCellValue(termA);
		
		sheet = workbookout.getSheet("Group-TermB");
		if(i == 1)
			row = sheet.getRow(i);
			else
				row = sheet.createRow(i);
		row.createCell(1);
		row.createCell(2);
		row.getCell(1).setCellValue(key);
		row.getCell(2).setCellValue(termB);
		
		sheet = workbookout.getSheet("TermA-TermB");
		if(i == 1)
			row = sheet.getRow(i);
			else
				row = sheet.createRow(i);
		row.createCell(1);
		row.createCell(2);
		row.createCell(3);
		row.getCell(1).setCellValue(termA);
		row.getCell(2).setCellValue(termB);
		row.getCell(3).setCellValue(similarityScore);
		
		
	}

	private static void establishloader2(HSSFWorkbook workbookout) {
		workbookout.createSheet("Loader");
		workbookout.createSheet("Group-TermA");
		workbookout.createSheet("Group-TermB");
		workbookout.createSheet("TermA-TermB");
		workbookout.createSheet("TermA-AutoMappedTerm");
		HSSFSheet sheet = workbookout.getSheet("Loader");
		Row row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Sheetname");
		row.createCell(1);
		row.getCell(1).setCellValue("Type");
		
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Group-TermA");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		
		row = sheet.createRow(2);
		row.createCell(0);
		row.getCell(0).setCellValue("Group-TermB");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		
		row = sheet.createRow(3);
		row.createCell(0);
		row.getCell(0).setCellValue("TermA-TermB");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		
		row = sheet.createRow(4);
		row.createCell(0);
		row.getCell(0).setCellValue("TermA-AutoMappedTerm");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		
		
		//next sheet
		sheet = workbookout.getSheet("Group-TermA");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("Group");
		row.createCell(2);
		row.getCell(2).setCellValue("TermA");
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("contains");
		//next sheet
		sheet = workbookout.getSheet("Group-TermB");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("Group");
		row.createCell(2);
		row.getCell(2).setCellValue("TermB");
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Contains");
		//next sheet
		
		sheet = workbookout.getSheet("TermA-TermB");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("TermA");
		row.createCell(2);
		row.getCell(2).setCellValue("TermB");
		row.createCell(3);
		row.getCell(3).setCellValue("weight");	
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Similarity");
		//next sheet
		
		sheet = workbookout.getSheet("TermA-AutoMappedTerm");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("TermA");
		row.createCell(2);
		row.getCell(2).setCellValue("AutoMappedTerm");
		row.createCell(3);
		row.getCell(3).setCellValue("weight");	
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Similarity");
		
		
	}

	private static void establishloader(HSSFWorkbook workbookout) {
		workbookout.createSheet("Loader");
		workbookout.createSheet("Group-TermA");
		workbookout.createSheet("Group-TermB");
		workbookout.createSheet("TermA-TermB");
		HSSFSheet sheet = workbookout.getSheet("Loader");
		Row row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Sheetname");
		row.createCell(1);
		row.getCell(1).setCellValue("Type");
		
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Group-TermA");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		
		row = sheet.createRow(2);
		row.createCell(0);
		row.getCell(0).setCellValue("Group-TermB");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		
		row = sheet.createRow(3);
		row.createCell(0);
		row.getCell(0).setCellValue("TermA-TermB");
		row.createCell(1);
		row.getCell(1).setCellValue("Usual");
		//next sheet
		sheet = workbookout.getSheet("Group-TermA");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("Group");
		row.createCell(2);
		row.getCell(2).setCellValue("TermA");
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("contains");
		//next sheet
		sheet = workbookout.getSheet("Group-TermB");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("Group");
		row.createCell(2);
		row.getCell(2).setCellValue("TermB");
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Contains");
		//next sheet
		sheet = workbookout.getSheet("TermA-TermB");
		row = sheet.createRow(0);
		row.createCell(0);
		row.getCell(0).setCellValue("Relation");
		row.createCell(1);
		row.getCell(1).setCellValue("TermA");
		row.createCell(2);
		row.getCell(2).setCellValue("TermB");
		row.createCell(3);
		row.getCell(3).setCellValue("weight");	
		row = sheet.createRow(1);
		row.createCell(0);
		row.getCell(0).setCellValue("Similarity");
		
		
	}


	private static String[] ExcelReader(ArrayList<String> listA) throws InvalidFormatException, IOException {
		int NumRows = 44;
		InputStream inp = new FileInputStream("C:\\Users\\sabidi\\workspace\\TermNormalization\\TermSet.xlsx");
		Workbook wb = WorkbookFactory.create(inp);
		Sheet sheet = wb.getSheetAt(0);
		Row row = sheet.getRow(1);
		for(int i = 1; i<NumRows; i++){
		row = sheet.getRow(i);
		listA.add(row.getCell(0).toString());
		}
		return null;
		
	}
	
	private static void ExcelReadRelationship(Hashtable<String, ArrayList<String>> relationHash, ArrayList<String> orphanList) throws InvalidFormatException, IOException {
		//This reads a sheet that has column A relate to Column B with the first row showing the object type of the two rows. 
		//!! set NumRows to the number of rows you are interested in analyzing
		System.out.println("ReadRelationship");
		int NumRows =  4148;	//4148
		int NumRows2 = 3847;  //3847
		InputStream inp = new FileInputStream("C:\\Users\\sabidi\\workspace\\TermNormalization\\ICDtermSet.xlsx");
		Workbook wb = WorkbookFactory.create(inp);
		Sheet sheet = wb.getSheet("Sheet4");//get the sheet where you want to pull information
		Row row = sheet.getRow(1);
		
		//Store relationship ObjectA>has>ItemB as a hashtable with the key being String ObjectA
		System.out.println("Getting ObjectA>has>ItemB");
		for(int i = 0; i<NumRows; i++){
		ArrayList<String> temp = new ArrayList<String>();
		row = sheet.getRow(i);
	//	System.out.println(" i value " + i + row.getCell(1).toString());
	//	relationHash.put(row.getCell(0).toString(), row.getCell(1).toString());
		if(relationHash.containsKey(row.getCell(0).toString())){
			
			relationHash.get(row.getCell(0).toString()).add(row.getCell(1).toString());
		}
		else{
			temp.add(row.getCell(1).toString());
			relationHash.put(row.getCell(0).toString(),temp);
		}
		}
		//Load Orphaned ItemsC into Arraylist
		System.out.println("Getting Orphaned Items");
		for(int i = 0; i<NumRows2; i++){
			row = sheet.getRow(i);	
			orphanList.add(row.getCell(2).toString());
		}
		System.out.println("Done Reading Input Excel");
		
	}

}
