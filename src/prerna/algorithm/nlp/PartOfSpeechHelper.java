package prerna.algorithm.nlp;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import edu.stanford.nlp.ling.LabeledWord;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TreeGraphNode;
import edu.stanford.nlp.trees.TypedDependency;

public final class PartOfSpeechHelper {

	private PartOfSpeechHelper() {

	}
	
	/**
	 * Generate a key-value mapping between the grammatical relation to the word from the sentence
	 * @param tdl			The type dependency list for every word in the sentence
	 * @param nodeHash		The Hashtable to put the key-value mapping from the type dependency list
	 */
	public static void setTypeDependencyHash(List<TypedDependency> tdl, Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		int i = 0;
		int size = tdl.size();
		for(; i < size; i++)
		{
			TypedDependency one = tdl.get(i);
			Vector<TypedDependency> baseVector = new Vector<TypedDependency>();
			GrammaticalRelation rel = one.reln();

			//if this type of relation already exists
			if(nodeHash.containsKey(rel)) {
				baseVector = nodeHash.get(rel);
			}
			baseVector.addElement(one);
			nodeHash.put(rel, baseVector);
		}
	}

	/**
	 * 
	 * @param dobjV
	 * @param subjV
	 * @param nodeHash
	 * @param subjR
	 * @param objR
	 * @return
	 */
	public static TreeGraphNode findPrepObject(Vector<TypedDependency> dobjV, Vector<TypedDependency> subjV, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, GrammaticalRelation subjR, GrammaticalRelation objR) {
		// based on the subjects and objects now find the predicates
		dobjV = nodeHash.get(objR);
		subjV = nodeHash.get(subjR);

		if (dobjV != null && subjV != null) {
			for (int dobjIndex = 0; dobjIndex < dobjV.size(); dobjIndex++) {
				TreeGraphNode pobj = dobjV.get(dobjIndex).dep();
				TreeGraphNode prep = dobjV.get(dobjIndex).gov();

				// now find the subject
				for (int subjIndex = 0; subjIndex < subjV.size(); subjIndex++) {
					TreeGraphNode prep2 = subjV.get(subjIndex).dep();
					if ((prep2 + "").equalsIgnoreCase(prep + "")) // this is the comparison to determine if there is a chain
						return pobj;
				}
			}
		}
		return null;
	}

	/**
	 * 
	 * @param subj
	 * @param nodeHash
	 * @return
	 */
	// finds the expanded object
	public static TreeGraphNode findCompObject(TreeGraphNode governor, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		TreeGraphNode retNode = governor;
		Vector <TypedDependency> compVector = nodeHash.get(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
		if(compVector != null) {
			int i = 0;
			int size = compVector.size();
			for(; i < size; i++) {
				TypedDependency td = compVector.elementAt(i);
				if(td.dep() == retNode) {
					retNode = td.gov();
					break;
				}
			}
		}

		compVector = nodeHash.get(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
		if(compVector != null) {
			int i = 0;
			int size = compVector.size();
			for(; i < size; i++) {
				TypedDependency td = compVector.elementAt(i);
				if(td.dep() == retNode) {
					retNode = td.gov();
					break;
				}
			}
		}
		return retNode;
	}

	/**
	 * 
	 * @param subj
	 * @param nodeHash
	 * @return
	 */
	// find expanded subject
	public static TreeGraphNode findCompSubject(TreeGraphNode subj, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		TreeGraphNode retNode = subj;
		Vector <TypedDependency> compVector = nodeHash.get(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
		boolean subjFound = false;
		if(compVector != null)
		{
			for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
			{
				TypedDependency td = compVector.elementAt(cInd);
				if(td.dep() == retNode)
				{
					retNode = td.gov();
					subjFound = true;
				}
			}
			compVector = nodeHash.get(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
			subjFound = false;
			if(compVector !=null)
			{
				for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
				{
					TypedDependency td = compVector.elementAt(cInd);
					if(td.dep() == retNode)
					{
						retNode = td.gov();
						subjFound = true;
					}
				}
				compVector = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
				subjFound = false;
				if(compVector != null){
					for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
					{
						TypedDependency td = compVector.elementAt(cInd);
						if(td.gov() == retNode)
						{
							retNode = td.dep();
							subjFound = true;
						}
					}
				}

				return retNode;
			}
		}
		return retNode;
	}

	/**
	 * 
	 * @param subj
	 * @param dep2
	 * @param nodeHash
	 * @param relation
	 * @return
	 */
	//sometimes the DAMN complement is recursive
	public static TreeGraphNode findComplementNoun(TreeGraphNode subj, TreeGraphNode dep2, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, GrammaticalRelation relation) {

		TreeGraphNode retNode = subj;
		// find all the complements
		// find the one where the dep is the same as dep passed through
		// now find a nsubj based on that new gov
		// start with CComplement
		Vector <TypedDependency> compVector = nodeHash.get(relation);
		if(compVector != null)
		{
			for(int cInd = 0;cInd < compVector.size();cInd++)
			{
				TypedDependency td = compVector.elementAt(cInd);
				TreeGraphNode dep = td.dep();
				TreeGraphNode gov = td.gov();
				if(dep == dep2)
				{
					// now find the nsubj
					Vector <TypedDependency> subjVector = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
					for(int subIndex = 0;subIndex < subjVector.size();subIndex++)
					{
						TypedDependency subTd = subjVector.elementAt(subIndex);
						if(subTd.gov() == gov)
							retNode = subTd.dep();
					}
				}
			}
			return retNode;
		}
		return retNode;
	}

	/**
	 * 
	 * @param node
	 * @return
	 */
	public static String getFullNoun(TreeGraphNode node)
	{
		String finalObject = "";
		boolean npFound = false;
		TreeGraphNode parentSearcher = node;
		while(!npFound)
		{
			if(!parentSearcher.label().toString().startsWith("NP"))
			{
				System.out.println(parentSearcher.label().toString());
				if(parentSearcher.parent() instanceof TreeGraphNode)
					parentSearcher = (TreeGraphNode) parentSearcher.parent();
				else
				{
					npFound = true;
					parentSearcher = null;
				}
			}
			else 
			{
				System.out.println(parentSearcher.label().toString());
				npFound = true;
				List<LabeledWord> lw = parentSearcher.labeledYield();
				// if this is not a noun then I need find the actual proper noun
				// and it may be because there is a CCOMP or XCOMP with this label
				// or there is an amod with this label
				for(int labIndex = 0; labIndex < lw.size();labIndex++)
				{
					finalObject = finalObject + lw.get(labIndex).word();
					if(labIndex != lw.size() - 1) {
						finalObject += " ";
					}
				}
			}
		}
		return finalObject;
	}

	/**
	 * 
	 * @param noun
	 * @param nodeHash
	 * @return
	 */
	public static String findPrepNoun(TreeGraphNode noun, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		// given the preperator
		// complete the string
		String retString = noun.value();

		if(!nodeHash.containsKey(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER))
			return retString;
		Vector <TypedDependency> prepVector = nodeHash.get(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
		//prepVector.addAll(nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER));
		for(int prepIndex = 0;prepIndex < prepVector.size();prepIndex++)
		{
			TypedDependency tdl = prepVector.elementAt(prepIndex);
			TreeGraphNode gov = tdl.gov();
			TreeGraphNode dep = tdl.dep();
			if(noun == gov )
			{
				String fullNoun = getFullNoun(dep);
				if(fullNoun.equalsIgnoreCase(dep.value()))
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep, nodeHash);
				else
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep, nodeHash).replace(dep.value(), "");
			}
		}
		return retString;
	}

	/**
	 * 
	 * @param noun
	 * @param nodeHash
	 * @return
	 */
	public static String findPrepNounForPredicate(TreeGraphNode noun, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		// given the preperator
		// complete the string
		String retString = "";

		if(!nodeHash.containsKey(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER))
			return retString;
		Vector <TypedDependency> prepVector = nodeHash.get(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
		for(int prepIndex = 0;prepIndex < prepVector.size();prepIndex++)
		{
			TypedDependency tdl = prepVector.elementAt(prepIndex);
			TreeGraphNode gov = tdl.gov();
			TreeGraphNode dep = tdl.dep();
			if(noun == gov )
			{				 
				String fullNoun = getFullNoun(dep);
				if(fullNoun.equalsIgnoreCase(dep.value()))
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep, nodeHash);
				else
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun;
			}
		}
		return retString;
	}

}
