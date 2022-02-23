package prerna.om;

import java.util.ArrayList;
import java.util.List;

public class Variable {
	
	// basic class for having variable
	public enum LANGUAGE {R, PYTHON, JAVA};
	public enum OUTPUT {SCALAR, VECTOR};
	
	String expression;
	LANGUAGE lang = LANGUAGE.R; // defaulting to R
	OUTPUT out = OUTPUT.SCALAR; // defaulting to scalar
	List <Variable> depends = new ArrayList<Variable>(); // list of variable this variable depends on 
	List <String> frames = new ArrayList<String>();
	String name = null;
	String format = null;
	
	public void setExpression(String expression)
	{
		this.expression = expression;
	}
	
	public String getExpression()
	{
		return this.expression;
	}
	
	public LANGUAGE getLanguage()
	{
		return this.lang;
	}
	
	public void setLanguage(LANGUAGE lang)
	{
		this.lang = lang;
	}
	
	public void addFrame(String frame)
	{
		this.frames.add(frame);
	}

	public List<String> getFrames()
	{
		return this.frames;
	}
	
	public void addParent(Variable parentVariable)
	{
		this.depends.add(parentVariable);
	}
	
	public List <Variable> getDepends()
	{
		return this.depends;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	public String getName()
	{
		return this.name;
	}	
	
	public void setFrames(List<String> frames)
	{
		this.frames = frames;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}
	
	public static String getExtension(LANGUAGE language) {
		if(language == LANGUAGE.JAVA) {
			return "java";
		} else if(language == LANGUAGE.R) {
			return "R";
		} else if(language == LANGUAGE.PYTHON) {
			return "py";
		}
		
		return null;
	}
}

