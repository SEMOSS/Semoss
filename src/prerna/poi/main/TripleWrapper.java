/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.poi.main;

import java.util.ArrayList;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.trees.Tree;

public class TripleWrapper {
	
	//core of the triple
	String Obj1;
	String Pred;
	String Obj2;
	//core plus extremedies
	String Obj1exp;
	String Predexp;
	String Obj2exp;
	//TaggedWord class POS //should be able to get string form
	String Obj1POS;
	String PredPOS;
	String Obj2POS;
	//normalized predicate
	String NormPred;
	String FrameNetPred;
//	Tree TreeObj1;
//	Tree TreePred;
//	Tree TreeObj2;
	String ArticleNum;
	String Sentence;
	double Obj1num;
	double Prednum;
	double Obj2num;
	public String getSentence() {
		return Sentence;
	}
	public void setSentence(String sentence) {
		Sentence = sentence;
	}
	public double getObj1num() {
		return Obj1num;
	}
	public void setObj1num(double obj1num) {
		Obj1num = obj1num;
	}
	public double getPrednum() {
		return Prednum;
	}
	public void setPrednum(double prednum) {
		Prednum = prednum;
	}
	public double getObj2num() {
		return Obj2num;
	}
	public void setObj2num(double obj2num) {
		Obj2num = obj2num;
	}
	
	
	public String getArticleNum() {
		return ArticleNum;
	}
	public void setArticleNum(String articleNum) {
		ArticleNum = articleNum;
	}
	
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(Obj1);
		result.append(">");
		result.append(Pred);
		result.append(">");
		result.append(Obj2);
		result.append(";");
		result.append(Obj1exp);
		result.append(">");
		result.append(Predexp);
		result.append(">");
		result.append(Obj2exp);
		result.append(";");
		result.append(Obj1POS);
		result.append(">");
		result.append(PredPOS);
		result.append(">");
		result.append(Obj2POS);
		result.append("\n");
		
		return result.toString();
	}
	public String getObj1() {
		return Obj1;
	}
	public void setObj1(String obj1) {
		Obj1 = obj1;
		
	}
	public String getPred() {
		return Pred;
	}
	public void setPred(String pred) {
		Pred = pred;
	}
	public String getObj2() {
		return Obj2;
	}
	public void setObj2(String obj2) {
		Obj2 = obj2;
	}
	public String getObj1exp() {
		return Obj1exp;
	}
	public void setObj1exp(String obj1exp) {
		Obj1exp = obj1exp;
	}
	public String getPredexp() {
		return Predexp;
	}
	public void setPredexp(String predexp) {
		Predexp = predexp;
	}
	public String getObj2exp() {
		return Obj2exp;
	}
	public void setObj2exp(String obj2exp) {
		Obj2exp = obj2exp;
	}
	public String getObj1POS() {
		return Obj1POS;
	}
	public void setObj1POS(String obj1, ArrayList<TaggedWord> taggedWords) {
		int j;
		String POS = null;
		POS = "Value didn't fill";
		for(int i = 0; i<taggedWords.size(); i++){
			j = i+1;
			String Arrayword = taggedWords.get(i).word().concat("-"+j);
			String  triplepart = obj1;
			if(Arrayword.equals(triplepart))
			{
				POS = taggedWords.get(i).tag().toString();
			}
		}
		Obj1POS = POS;
	}
	public String getPredPOS() {
		return PredPOS;
	}
	public void setPredPOS(String pred, ArrayList<TaggedWord> taggedWords) {
		int j;
		String POS = null;
		POS = "value didn't fill";
		for(int i = 0; i<taggedWords.size(); i++){
			j = i+1;
			String Arrayword = taggedWords.get(i).word().concat("-"+j);
		//	System.out.println("TEST POS");
		//	System.out.println(Arrayword);
		//	System.out.println(pred);
			String  triplepart = pred;
			triplepart =triplepart.substring(triplepart.length()-1);
			if(Arrayword.substring(Arrayword.length()-1).equals(triplepart.substring(triplepart.length()-1)))
			{
				POS = taggedWords.get(i).tag().toString();;
			}
		}
		PredPOS = POS;
	}
	public String getObj2POS() {
		return Obj2POS;
	}
	public void setObj2POS(String obj2, ArrayList<TaggedWord> taggedWords) {
		int j;
		String POS = null;
		POS = "value didn't fill";
		for(int i = 0; i<taggedWords.size(); i++){
			j = i+1;
			String Arrayword = taggedWords.get(i).word().concat("-"+j);
			String  triplepart = obj2;
			if(Arrayword.equals(triplepart))
			{
				POS = taggedWords.get(i).tag().toString();
			}
		}
		Obj2POS = POS;
	}
	public String getNormPred() {
		return NormPred;
	}
	public void setNormPred(String normPred) {
		NormPred = normPred;
	}
	public String getFrameNetPred() {
		return FrameNetPred;
	}
	public void setFrameNetPred(String frameNetPred) {
		FrameNetPred = frameNetPred;
	}
//	public Tree getTreeObj1() {
//		return TreeObj1;
//	}
//	public void setTreeObj1(Tree treeObj1) {
//		TreeObj1 = treeObj1;
//	}
//	public Tree getTreePred() {
//		return TreePred;
//	}
//	public void setTreePred(Tree treePred) {
//		TreePred = treePred;
//	}
//	public Tree getTreeObj2() {
//		return TreeObj2;
//	}
//	public void setTreeObj2(Tree treeObj2) {
//		TreeObj2 = treeObj2;
//	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((FrameNetPred == null) ? 0 : FrameNetPred.hashCode());
		result = prime * result + ((Obj1 == null) ? 0 : Obj1.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TripleWrapper other = (TripleWrapper) obj;
		if (FrameNetPred == null) {
			if (other.FrameNetPred != null)
				return false;
		} else if (!FrameNetPred.equals(other.FrameNetPred))
			return false;
		if (Obj1 == null) {
			if (other.Obj1 != null)
				return false;
		} else if (!Obj1.equals(other.Obj1))
			return false;
		return true;
	}
	public TripleWrapper()
	{
		
	}

}
