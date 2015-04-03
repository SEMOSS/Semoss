package prerna.rdf.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import rita.RiTa;
import rita.RiWordNet;

public class ComparingTwoColumns {

	private static RiWordNet wordnet;
	
	private static String[] headers;
	private static List<String> colA;
	private static List<String> colB;
	
	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		///////////////////////////////////////////////////
		//TODO: change to correct file location when testing
		String loc = "C:\\Users\\mahkhalil\\Desktop\\ComparingTwoColumns.xlsx";
		String wordNetLoc = "C:\\Users\\mahkhalil\\workspace\\Semoss_Dev\\RDFGraphLib\\WordNet-3.1";
		///////////////////////////////////////////////////

		wordnet = new RiWordNet(wordNetLoc, false, true);
		
		readExcelFile(loc);
		System.out.println("Headers: " + Arrays.toString(headers));
		System.out.println("Column A: " + colA);
		System.out.println("Column B: " + colB);
		
		System.out.println("");
		
		// get unique values of A
		Set<String> uniqueA = new HashSet<String>();
		uniqueA.addAll(colA);
		System.out.println("Unique A: " + uniqueA);

		// get unique values of B
		Set<String> uniqueB = new HashSet<String>();
		uniqueB.addAll(colB);
		System.out.println("Unique B: " + uniqueB);

		System.out.println("");
		
		// determine most dissimilar entities in A
		String[] mostSimilarA = determineMostSimilarValues(uniqueA);
		System.out.println("Most similar in A: " + Arrays.toString(mostSimilarA));
		double mostSimA = wordnet.getDistance(RiTa.singularize(mostSimilarA[0].toLowerCase()), RiTa.singularize(mostSimilarA[1].toLowerCase()), "n");
		System.out.println("Most similar value in A: " + mostSimA);
		
		// determine most dissimilar entities in B
		String[] mostSimilarB = determineMostSimilarValues(uniqueB);
		System.out.println("Most similar in B: " + Arrays.toString(mostSimilarB));
		double mostSimB = wordnet.getDistance(RiTa.singularize(mostSimilarB[0].toLowerCase()), RiTa.singularize(mostSimilarB[1].toLowerCase()), "n");
		System.out.println("Most similar value in B: " + mostSimB);
		System.out.println("");
		
		boolean aLarger = false;
		if(mostSimA > mostSimB) {
			aLarger = true;
			System.out.println("Column A has a wider gap in its values and should be used as the new base column");
		} else {
			System.out.println("Column B has a wider gap in its values and should be used as the new base column");
		}
		
		double simBarrier = Math.max(mostSimA, mostSimB)/2.0;
		System.out.println("Similarity barrier: " + simBarrier);
		System.out.println("");
		
		List<String> newList = new ArrayList<String>();
		if(aLarger) {
			newList.addAll(colA);
		} else {
			newList.addAll(colB);
		}
		
		if(aLarger) {
			for(String s1 : colB) {
				boolean foundMatch = false;
				double bestSim = simBarrier;
				for(String s2 : colA) {
					double newSim = wordnet.getDistance(RiTa.singularize(s1.toLowerCase()), RiTa.singularize(s2.toLowerCase()), "n");
					if(newSim < bestSim) {
						foundMatch = true;
						bestSim = newSim;
						System.out.println("Value " + s1 + " in ColB and value " + s2 + " in ColA are a match!");
					}
				}
				
				if(!foundMatch) {
					newList.add(s1);
				}
			}
		} else {
			for(String s1 : colA) {
				boolean foundMatch = false;
				double bestSim = simBarrier;
				for(String s2 : colB) {
					double newSim = wordnet.getDistance(RiTa.singularize(s1.toLowerCase()), RiTa.singularize(s2.toLowerCase()), "n");
					if(newSim < bestSim) {
						foundMatch = true;
						bestSim = newSim;
						System.out.println("Value " + s1 + " in ColA and value " + s2 + " in ColB are a match!");
					}
				}
				
				if(!foundMatch) {
					newList.add(s1);
				}
			}
		}
	
		System.out.println("");
		System.out.println("New combined list would be: " + newList);
		
		long end = System.currentTimeMillis();

		System.out.println("Time in seconds = " + (end - start)/1000);
	}
	
//	public static void main(String[] args) {
//		///////////////////////////////////////////////////
//		//TODO: change to correct file location when testing
//		String loc = "C:\\Users\\mahkhalil\\Desktop\\ComparingTwoColumns.xlsx";
//		String wordNetLoc = "C:\\Users\\mahkhalil\\workspace\\Semoss_Dev\\RDFGraphLib\\WordNet-3.1";
//		///////////////////////////////////////////////////
//
//		wordnet = new RiWordNet(wordNetLoc, false, true);
//		
//		readExcelFile(loc);
//		System.out.println("Headers: " + Arrays.toString(headers));
//		System.out.println("Column A: " + colA);
//		System.out.println("Column B: " + colB);
//		
//		System.out.println("");
//		
//		// get unique values of A
//		Set<String> uniqueA = new HashSet<String>();
//		uniqueA.addAll(colA);
//		System.out.println("Unique A: " + uniqueA);
//
//		// get unique values of B
//		Set<String> uniqueB = new HashSet<String>();
//		uniqueB.addAll(colB);
//		System.out.println("Unique B: " + uniqueB);
//
//		System.out.println("");
//		
//		// determine most dissimilar entities in A
//		String[] mostDissimilarA = determineLeastSimilarValues(uniqueA);
//		System.out.println("Most Dissimilar in A: " + Arrays.toString(mostDissimilarA));
//		
//		// determine most dissimilar entities in B
//		String[] mostDissimilarB = determineLeastSimilarValues(uniqueB);
//		System.out.println("Most Dissimilar in B: " + Arrays.toString(mostDissimilarB));
//		
//		System.out.println("");
//		
//		// determine common parent of A
//		String[] mostCommonParent_A = wordnet.getCommonParents(RiTa.singularize(mostDissimilarA[0].toLowerCase()), RiTa.singularize(mostDissimilarA[1].toLowerCase()), "n");
//		System.out.println("Closest Hypernum Set of most dissimilar values in A: " + Arrays.toString(mostCommonParent_A));
//		
//		String[] mostCommonParent_B = wordnet.getCommonParents(RiTa.singularize(mostDissimilarB[0].toLowerCase()), RiTa.singularize(mostDissimilarB[1].toLowerCase()), "n");
//		System.out.println("Closest Hypernum Set of most dissimilar values in B: " + Arrays.toString(mostCommonParent_B));
//
//		System.out.println("");
//		System.out.println("***Note: Only need to consider one entry in hypernum set since all values are synonyms with eachother");
//		
//		// synonym set of A's most common parent
//		ArrayList<String> synSetList_A = new ArrayList<String>(Arrays.asList(wordnet.getSynonyms(mostCommonParent_A[0], "n"))); // can grab first value as all outputs are synonyms
//		synSetList_A.addAll(Arrays.asList(wordnet.getHypernyms(mostCommonParent_A[0], "n")));
//		synSetList_A.addAll(Arrays.asList(wordnet.getHyponyms(mostCommonParent_A[0], "n")));
//		synSetList_A.add(mostCommonParent_A[0]);
//		
//		Set<String> synSetA = new HashSet<String>();
//		synSetA.addAll(synSetList_A);
//		System.out.println("Synonms for Closest Hypernym in A: " + synSetA);
//		
//		System.out.println("");
//		// synonym set of B's most common parent
//		ArrayList<String> synSetList_B = new ArrayList<String>(Arrays.asList(wordnet.getSynonyms(mostCommonParent_B[0], "n"))); // can grab first value as all outputs are synonyms
//		synSetList_B.addAll(Arrays.asList(wordnet.getHypernyms(mostCommonParent_B[0], "n")));
//		synSetList_B.addAll(Arrays.asList(wordnet.getHyponyms(mostCommonParent_B[0], "n")));
//		synSetList_B.add(mostCommonParent_B[0]);
//		
//		Set<String> synSetB = new HashSet<String>();
//		synSetB.addAll(synSetList_B);
//		System.out.println("Synonms for Closest Hypernym in B: " + synSetB);
//		
//		System.out.println("");
//		
//		boolean match = false;
//		for(String s1 : synSetA) {
//			if(synSetB.contains(s1)) {
//				match = true;
//				System.out.println("Found match in both sets");
//				break;
//			}
//		}
//		System.out.println("Do synonym sets overlap? " + match);
//	}
	
	public static String[] determineLeastSimilarValues(Set<String> col) {
		Object[] colList = col.toArray();
		
		String[] mostDissimilarSet = null;
		double simVal = -1.0; // set to arbitrary value less than 0
		
		int i = 0;
		int size = colList.length;
		for(; i < size; i++) {
			String s1 = colList[i].toString();
			String s_s1 = RiTa.singularize(s1.toLowerCase());
			int j = i+1;
			// compare against all other strings in col
			for(; j < size; j++) {
				String s2 = colList[j].toString();
				String s_s2 = RiTa.singularize(s2.toLowerCase());
				double newSim = wordnet.getDistance(s_s1, s_s2, "n");
				if(wordnet.getDistance(s_s1, s_s2, "n") > simVal) {
					mostDissimilarSet = new String[]{s1, s2};
					simVal = newSim;
				}
			}
		}
		
		return mostDissimilarSet;
	}
	
	public static String[] determineMostSimilarValues(Set<String> col) {
		Object[] colList = col.toArray();
		
		String[] mostSimilarSet = null;
		double simVal = 2.0; // set to arbitrary value larger than 1
		
		int i = 0;
		int size = colList.length;
		for(; i < size; i++) {
			String s1 = colList[i].toString();
			String s_s1 = RiTa.singularize(s1.toLowerCase());
			int j = i+1;
			// compare against all other strings in col
			for(; j < size; j++) {
				String s2 = colList[j].toString();
				String s_s2 = RiTa.singularize(s2.toLowerCase());
				double newSim = wordnet.getDistance(s_s1, s_s2, "n");
				if(newSim < simVal) {
					mostSimilarSet = new String[]{s1, s2};
					simVal = newSim;
				}
			}
		}
		
		return mostSimilarSet;
	}
	
	public static void readExcelFile(String loc) {
		FileInputStream is = null;
		try {
			is = new FileInputStream(loc);
			XSSFWorkbook wb = new XSSFWorkbook(is);
			XSSFSheet xs = wb.getSheetAt(0);
			
			colA = new ArrayList<String>();
			colB = new ArrayList<String>();
			
			// get header row and store values
			XSSFRow headerRow = xs.getRow(0);
			headers = new String[]{headerRow.getCell(0).getStringCellValue(), headerRow.getCell(1).getStringCellValue()};
			
			// loop through and store all values
			int size = xs.getLastRowNum();
			int i = 1;
			for(; i <= size; i++) {
				XSSFRow currRow = xs.getRow(i);
				if(currRow != null) {
					if(currRow.getCell(0) != null) {
						colA.add(currRow.getCell(0).getStringCellValue());
					}
					if(currRow.getCell(1) != null) {
						colB.add(currRow.getCell(1).getStringCellValue());
					}
				}
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
}
