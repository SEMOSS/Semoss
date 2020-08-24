package prerna.query.querystruct;

import java.util.List;
import java.util.Vector;

public class WhenExpression extends GenExpression {
	
	// sets the condition
	String elseClause = null;
	
	List <String> whens = new Vector<String>();
	List <String> thens = new Vector<String>();
	
	public void addWhenThen(String when, String then)
	{
		whens.add(when);
		thens.add(then);
	}
	
	public void setElse(String elseClause)
	{
		this.elseClause = elseClause;
	}
	
	public StringBuffer printOutput()
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
	

}
