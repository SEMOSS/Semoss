package prerna.query.querystruct;

import java.util.List;
import java.util.Vector;

public class WhenExpression extends GenExpression {
	
	// sets the condition
	String elseClause = null;
	
	List <String> whens = new Vector<String>();
	List <String> thens = new Vector<String>();
	
	List <GenExpression> whenE = new Vector <GenExpression>();
	List <GenExpression> thenE = new Vector <GenExpression>();
	GenExpression elseE = null;
	
	public void addWhenThen(String when, String then)
	{
		whens.add(when);
		thens.add(then);
	}
	
	public void setElse(String elseClause)
	{
		this.elseClause = elseClause;
	}
	
	public void addWhenThenE(GenExpression when, GenExpression then)
	{
		whenE.add(when);
		thenE.add(then);

	}
	
	public void setElseE(GenExpression elseE)
	{
		this.elseE = elseE;
	}
	
	public StringBuffer printOutput2()
	{
		StringBuffer output = new StringBuffer();
		
		if(whens.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < whens.size();whenIndex++)
			{
				output.append("when " ).append(whens.get(whenIndex)).append(" then ").append(thens.get(whenIndex)).append(" ");
			}
			
			if(elseClause != null)
			{
				output.append(" else ").append(elseClause);
			}
			output.append(" end");
			
			if(this.leftAlias != null)
				output.append(" AS " + this.leftAlias);
		}
		
		return output;
	}

	public StringBuffer printOutput()
	{
		StringBuffer output = new StringBuffer();
		
		if(whens.size() > 0)
		{
			output.append("case ");
			
			for(int whenIndex = 0;whenIndex < whenE.size();whenIndex++)
			{
				GenExpression thisWhen = whenE.get(whenIndex);
				StringBuffer whenBuf = new StringBuffer();
				whenBuf = thisWhen.printQS(thisWhen, whenBuf);

				GenExpression thisThen = thenE.get(whenIndex);
				StringBuffer thenBuf = new StringBuffer();
				thenBuf = thisThen.printQS(thisThen, thenBuf);

				//System.err.println("When ...  " + whenBuf);
				//System.err.println("Then ...  " + thenBuf);

				output.append("when " ).append(whenBuf).append(" then ").append(thenBuf).append(" ");
			}
			
			if(elseE != null)
			{
				StringBuffer elseBuf = new StringBuffer();
				elseBuf = elseE.printQS(elseE, elseBuf);
				output.append(" else ").append(elseBuf);
			}
			output.append(" end");
			
			if(this.leftAlias != null)
				output.append(" AS " + this.leftAlias);
		}
		
		return output;
	}


}
