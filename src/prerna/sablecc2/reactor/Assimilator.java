package prerna.sablecc2.reactor;

import java.util.Vector;

import prerna.sablecc2.om.Expression;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PkslDataTypes;

/**
 * 	NounStore:
 *		key -> GenrowStruct
 *		where each key is a signature and the GenRowStruct contains either a string, lambda, or expression 
 *		
 *		so for the case of 2 + ( 3 + X(5) )
 *		
 *		AssimilatorA's NounStore:
 *		'left' -> '2'
 *		'right' -> '( 3 + X(5))'
 *		'operation' -> '+'
 *		'2' -> '2'
 *		'(3 + X (5))' -> AssimilatorB
 *			
 *			AssimilatorB's NounStore
 *			'left' -> '3'
 *			'right' -> 'X(5)'
 *			'operation' -> '+'
 *			'3' -> '3'
 *			'X(5)' -> Lambda
 *		
 *		Out() method will produce an expression by iterating recursively through the left and right to produce a single expression which can be executed
 *
 */
public class Assimilator extends AbstractReactor {
	
	// roles of the assimilator is simple, just assimilate an expression and then
	// plug it into the parent
	// filter is a good example of assimilator for example

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	/**
	 * 
	 */
	public Object Out() {
		Expression thisExpression = getExpression();
		this.parentReactor.getCurRow().addE(thisExpression);
		return parentReactor;
	}

	private Expression getExpression() {
		Vector<String> inputColumns = curRow.getAllColumns();
		String [] allColumns = new String[inputColumns.size()];
		for(int colIndex = 0;colIndex < inputColumns.size(); colIndex++) {
			allColumns[colIndex] = inputColumns.elementAt(colIndex)+"";
		}
		// the expression will just store the entire signature and the columns
		// the columns is added because as we keep going through the parsing
		// columns definitions just get appended into the GenRowStruct curRow 
		Expression thisExpression = new Expression(signature, allColumns);
		thisExpression.setLeft(getSubExpression("LEFT")); //get the left side and set it
		thisExpression.setRight(getSubExpression("RIGHT")); //get the right side and set it
		thisExpression.setOperation(getOperation()); //get the operation string and set it
		return thisExpression;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * 
	 * This method recursively builds the expression for each side
	 */
	private Object getSubExpression(String key) {
		//key will be 'LEFT' or 'RIGHT'
		GenRowStruct exprNoun = this.getNounStore().getNoun(key);
		
		//exprNounKey will be the signature of the operation
		String exprNounKey = (String)exprNoun.get(0);
		
		//lets see if we have a lambda stored under the signature
		GenRowStruct expr = this.getNounStore().getNoun(exprNounKey);
		
		//if we don't return the key, this will be the form of '2' or 'x' or something like that
		if(expr == null) {
			return exprNounKey;
		}
		
		//if we have a key it will be either an assimilator or a labda (i.e. reactor) or a nodekey (meaning we need to look again)
		//Ex: 2 + (3 + Sum(MB))
		// left will be '2'
		// right will be '(3 + Sum(MB))'
		//		but the assimilator will be stored under the key '3 + Sum(MB)' since that is the logical start point for the assimilator in translation
		//		so we then must redirect '(3 + Sum(MB)' to '3 + Sum(MB)'
		//		we do this by adding a key/value pair in the nounstore to say: '(3 + Sum(MB)' -> '3 + Sum(MB)'
		//		this is done in outAFormula of translation
		//		we identify that this is redirection by the pkslDataType being a NODEKEY
		while(PkslDataTypes.NODEKEY.equals(expr.getMeta(0))) {
			exprNounKey = (String)expr.get(0);
			expr = this.getNounStore().getNoun(exprNounKey);
			if(expr == null) {
				return exprNounKey;
			}
		}
		
		//We have found an object, if its an assimilator then return the expression that will be built from that assimilator
		Object exprObj = expr.get(0);
		if(exprObj instanceof Assimilator) {
			return ((Assimilator) exprObj).getExpression();
		}
		
		//else just return the object which in this case is a lambda
		return expr.get(0);
	}
	
	private String getOperation() {
		GenRowStruct operation = this.getNounStore().getNoun("OPERATOR");
		String op = (String)operation.get(0);
		return op;
	}
	
	@Override
	protected void mergeUp() {

	}

	@Override
	protected void updatePlan() {
	
	}
}
