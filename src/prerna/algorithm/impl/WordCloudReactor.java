package prerna.algorithm.impl;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;


public class WordCloudReactor extends MathReactor {
		
	public WordCloudReactor() {
		setMathRoutine("WordCloud");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);

		ITableDataFrame df = (ITableDataFrame)  myStore.get("G");
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		
		List<String> stopWords = Arrays.asList("a","about","above","after","again","against",
				"all","am","an","and","any","are","aren't","as","at","be","became","because","been","before",
				"being","below","between","both","but","by","came","can","can't","cannot","could","couldn't","did",
				"didn't","do","does","doesn't","doing","don't","down","during","each","e.g.","few","for","from",
				"further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll",
				"he's","her","here","here's","hers","herself","him","himself","his","how","how's",
				"i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","include",
				"let's","many","me","more","most","much","mustn't","my","myself","no","nor","not","of","off","on",
				"once","only","or","other","ought","our","ours	ourselves","out","over","own","same",
				"shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than",
				"that","that's","the","their","theirs","them","themselves","then","there","there's",
				"these","they","they'd","they'll","they're","they've","this","those","through","to",
				"too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've",
				"were","weren't","what","what's","when","when's","where","where's","which","went","while",
				"who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd",
				"you'll","you're","you've","your","yours","yourself","yourselves");
		
		String pattern = "^[A-Za-z0-9].*";
		
		Iterator inputItr = getTinkerData(columns, df, false);
		HashMap<String,Double> resultMap = new HashMap<>();
		MaxentTagger tagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
		List<String> filters = null;
		if (options.containsKey("FILTERS"))
			filters = Arrays.asList(options.get("FILTERS").toString().split(","));
		
		while (inputItr.hasNext()){
			Object[] input = (Object[])inputItr.next();
			if (input == null || input[0].equals(""))
				continue;
			String inputString = (String)input[0]; //new String(((String)input[0]).getBytes(),"UTF-8");			
			if (options.get("PREPROCESS").equals("false")){
					incrementKeyValueInMap(resultMap, inputString);
			}else{
				StringReader sr = new StringReader(inputString);
				DocumentPreprocessor documentPreprocesor = new DocumentPreprocessor(sr);
				for(List<HasWord> sentence : documentPreprocesor){
					ArrayList<TaggedWord> taggedWords = tagger.tagSentence(sentence);
					for(TaggedWord word : taggedWords){
						if (stopWords.contains(word.value().toLowerCase()) || !Pattern.matches(pattern, word.value()))
							continue;
						if (filters != null){
							for(String filter : filters){
								if(word.tag().startsWith(filter))
									incrementKeyValueInMap(resultMap, word.value());
							}
						}else
							incrementKeyValueInMap(resultMap, word.value());
					}
				}
			}
		}
		List<Entry<String,Double>> entries = new ArrayList<Entry<String,Double>>(resultMap.size());
		for(Entry<String,Double> entry : resultMap.entrySet()){
			entries.add(entry);
		}
		Comparator<Entry<String,Double>> compareEntriesByValue = new Comparator<Map.Entry<String,Double>>() {
			
			@Override
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				// TODO Auto-generated method stub
				Double v1 = o1.getValue();
				Double v2 = o2.getValue();
				return v2.compareTo(v1);
			}
		};
		Collections.sort(entries,compareEntriesByValue);
		
		int numWords = (options != null) && options.containsKey("NUMWORDS") ? Integer.parseInt(options.get("NUMWORDS").toString()) : entries.size();
		
		HashMap<String,Double> wordCloudMap = new LinkedHashMap<>();
		for(Entry<String,Double> entry: entries){
			if (numWords <= 0)
				break;
			wordCloudMap.put(entry.getKey(),entry.getValue());
			numWords--;
		}
		
		String nodeStr = myStore.get(whoAmI).toString();
		HashMap<String,Object> additionalInfo = new HashMap<>();
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		additionalInfo.put("WordCloud", wordCloudMap);
		myStore.put(nodeStr, null);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
	private void incrementKeyValueInMap(Map<String,Double> map, String key){
		if (map.containsKey(key))
	    	map.put(key, map.get(key) + 1.0);
	    else
	    	map.put(key, 1.0);
	}

}

/**
package prerna.algorithm.impl;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Vector;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;


public class WordCloudReactor extends MathReactor {
	
	String op = "SUM";
	
	public WordCloudReactor() {
		setMathRoutine("WordCloud");
	}
	
	@Override
	public Iterator process() {
		modExpression();
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		
		if(myStore.containsKey(PKQLEnum.COL_CSV)){
			Vector<String> valueColumn = (Vector <String>) myStore.get(PKQLEnum.COL_CSV);
			if (valueColumn != null)
				columns.addAll(valueColumn);
		}

		ITableDataFrame df = (ITableDataFrame)  myStore.get("G");
		
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		
		if (options.containsKey("OP"))
			this.op = String.valueOf(options.get("OP"));
		
		List<String> stopWords = Arrays.asList("a","about","above","after","again","against",
				"all","am","an","and","any","are","aren't","as","at","be","became","because","been","before",
				"being","below","between","both","but","by","came","can","can't","cannot","could","couldn't","did",
				"didn't","do","does","doesn't","doing","don't","down","during","each","e.g.","few","for","from",
				"further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll",
				"he's","her","here","here's","hers","herself","him","himself","his","how","how's",
				"i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","include",
				"let's","many","me","more","most","much","mustn't","my","myself","no","nor","not","of","off","on",
				"once","only","or","other","ought","our","ours	ourselves","out","over","own","same",
				"shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than",
				"that","that's","the","their","theirs","them","themselves","then","there","there's",
				"these","they","they'd","they'll","they're","they've","this","those","through","to",
				"too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've",
				"were","weren't","what","what's","when","when's","where","where's","which","went","while",
				"who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd",
				"you'll","you're","you've","your","yours","yourself","yourselves");
		
		String pattern = "^[A-Za-z0-9].*";
		
		Iterator inputItr = getTinkerData(columns, df, false);
		HashMap<String,Double> resultMap = new HashMap<>();
		MaxentTagger tagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
		List<String> filters = null;
		if (options.containsKey("FILTERS"))
			filters = Arrays.asList(options.get("FILTERS").toString().split(","));
		
		while (inputItr.hasNext()){
			Object[] input = (Object[])inputItr.next();
			if (input == null || input[0].equals(""))
				continue;
			String inputString = (String)input[0]; //new String(((String)input[0]).getBytes(),"UTF-8");			
			if (options.get("PREPROCESS").equals("false")){
				if(input.length > 1)
					incrementKeyValueInMap(resultMap, inputString, Double.valueOf(String.valueOf(input[1])));
				else
					incrementKeyValueInMap(resultMap, inputString, 1.0);
			}else{
				StringReader sr = new StringReader(inputString);
				DocumentPreprocessor documentPreprocesor = new DocumentPreprocessor(sr);
				for(List<HasWord> sentence : documentPreprocesor){
					ArrayList<TaggedWord> taggedWords = tagger.tagSentence(sentence);
					for(TaggedWord word : taggedWords){
						if (stopWords.contains(word.value().toLowerCase()) || !Pattern.matches(pattern, word.value()))
							continue;
						if (filters != null){
							for(String filter : filters){
								if(word.tag().startsWith(filter))
									incrementKeyValueInMap(resultMap, word.value(),1.0);
							}
						}else
							incrementKeyValueInMap(resultMap, word.value(),1.0);
					}
				}
			}
		}
		List<Entry<String,Double>> entries = new ArrayList<Entry<String,Double>>(resultMap.size());
		for(Entry<String,Double> entry : resultMap.entrySet()){
			entries.add(entry);
		}
		Comparator<Entry<String,Double>> compareEntriesByValue = new Comparator<Map.Entry<String,Double>>() {
			
			@Override
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				// TODO Auto-generated method stub
				Double v1 = o1.getValue();
				Double v2 = o2.getValue();
				return v2.compareTo(v1);
			}
		};
		Collections.sort(entries,compareEntriesByValue);
		
		int numWords = (options != null) && options.containsKey("NUMWORDS") ? Integer.parseInt(options.get("NUMWORDS").toString()) : entries.size();
		
		HashMap<String,Double> wordCloudMap = new LinkedHashMap<>();
		for(Entry<String,Double> entry: entries){
			if (numWords <= 0)
				break;
			wordCloudMap.put(entry.getKey(),entry.getValue());
			numWords--;
		}
		
		String nodeStr = myStore.get(whoAmI).toString();
		HashMap<String,Object> additionalInfo = new HashMap<>();
		myStore.put("ADDITIONAL_INFO", additionalInfo);
		additionalInfo.put("WordCloud", wordCloudMap);
		myStore.put(nodeStr, null);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
	
	private void incrementKeyValueInMap(Map<String,Double> map, String key, Double increment){
		if (map.containsKey(key))
	    	map.put(key, updateValue(map.get(key), increment));
	    else
	    	map.put(key, 1.0);
	}
	
	private double updateValue(double oldValue, double newValue){
		switch(op){
		case "MAX":
			return Double.max(oldValue, newValue);
		case "SUM":
		default:
			return oldValue + newValue;
		}
	}
}
**/