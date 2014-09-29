package prerna.algorithm.nlp;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import prerna.util.Constants;
import prerna.util.DIHelper;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.util.CoreMap;

public class StanfordNLPfuncts{

    protected StanfordCoreNLP pipeline;
    static LexicalizedParser lp;

    public static void main(String[] args) throws InvalidFormatException, IOException {
    String test = "cats found doggy";
    StanfordNLPfuncts temp = new StanfordNLPfuncts();
    System.out.println(temp.lemmatize(test));;
    }
    
    public StanfordNLPfuncts() {
        // Create StanfordCoreNLP object properties, with POS tagging
        // (required for lemmatization), and lemmatization
        Properties props;
        props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");

        // StanfordCoreNLP loads a lot of models, so you probably
        // only want to do this once per execution
        this.pipeline = new StanfordCoreNLP(props);
		final String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String fileSeparator = System.getProperty("file.separator");
		String file =  baseDirectory + fileSeparator + "NLPartifacts" + fileSeparator + "englishPCFG.ser";
        lp = LexicalizedParser.loadModel(file);
		
    }

    public String lemmatize(String documentText)
    {
    	StringBuffer Lemmatized = new StringBuffer();
        List<String> lemmas = new LinkedList<String>();
        
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(documentText);

        // run all Annotators on this text
        this.pipeline.annotate(document);

        // Iterate over all of the sentences found
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
            // Iterate over all tokens in a sentence
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // Retrieve and add the lemma for each word into the list of lemmas
                lemmas.add(token.get(LemmaAnnotation.class));
                Lemmatized.append( token.get(LemmaAnnotation.class) + " ");
            }
        }
      //  System.out.println("Lemmatized " +Lemmatized);
        return Lemmatized.toString();
        //return lemmas;
    }
    
    public List<TypedDependency> CreateDepList(String TheSentence, List<TypedDependency> tdl, List<TaggedWord> TaggedWords)
	{
    	ArrayList <CoreLabel> CoreLabels = new ArrayList<CoreLabel>();
    	boolean SentenceParsable = true;
		//picking the grammer sheet to use for parsing

		TokenizerFactory<CoreLabel> tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
		////This structures the sentence - needs sentence as an input and would return a list of typedependencies
		List<CoreLabel> rawWords = tokenizerFactory.getTokenizer(new StringReader(TheSentence)).tokenize();
		Tree bestParse = lp.parseTree(rawWords);

		Tree parse = bestParse; 
		try{
			TaggedWords.addAll(bestParse.taggedYield()); //gives each word a POS
			SentenceParsable = true;
		}
		catch(NullPointerException e ){
			System.out.println("This Sentence failed: "+ TheSentence);
			SentenceParsable = false;
			return tdl;
		}
		CoreLabels.addAll( bestParse.taggedLabeledYield());
	//	System.out.println("From createDep: "+ TheSentence);
	//	System.out.println("From createDep: "+ TaggedWords);

		GrammaticalStructure gs = null;
		TreebankLanguagePack tlp = new PennTreebankLanguagePack();
		GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
		gs = gsf.newGrammaticalStructure(parse);

		tdl = gs.typedDependenciesCCprocessed(); //@@choose which set of dependencies you want
		return tdl;
	}
    
    public Hashtable<GrammaticalRelation, Vector<TypedDependency>> setHash(List<TypedDependency> tdl,Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHashA)
	{
		for(int tdlIndex = 0;tdlIndex < tdl.size();tdlIndex++)
		{
			TypedDependency one = tdl.get(tdlIndex);
			Vector <TypedDependency> baseVector = new Vector<TypedDependency>();
			GrammaticalRelation rel = one.reln();

			if(nodeHashA.containsKey(rel)) //if this type of relation already exists
				baseVector = nodeHashA.get(rel);
			baseVector.addElement(one);
			nodeHashA.put(rel, baseVector);
		}
		return nodeHashA;
	}

	public ArrayList<Double> TermPrioritization(String a, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, List<TypedDependency> tdl, ArrayList<String> phrase) {
		ArrayList <Double> TermRank = new ArrayList<Double>();
		Vector <TypedDependency> NN_relate = new Vector<TypedDependency>();
		Vector<TypedDependency> root = new Vector<TypedDependency>();
		
		//Primary case: all relations are NN && last term is root
		int headpos = -1;
		NN_relate = nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER);

		
		if(NN_relate != null && (NN_relate.size()+1) == (phrase.size())){
		root = nodeHash.get(GrammaticalRelation.ROOT);
		headpos = root.get(0).dep().index();
		}
		
		
		//assign weights to terms
		//System.out.println("HEADPOS "+ headpos);
		for(int i = 0; i<phrase.size(); i++){
			if(i == (headpos-1))
				TermRank.add(Math.sqrt(phrase.size()));
			else{
				TermRank.add(1.00);
			}
		}
		
		return TermRank;
		
		
		
	}
}