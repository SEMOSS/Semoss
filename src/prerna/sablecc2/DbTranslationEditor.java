package prerna.sablecc2;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class DbTranslationEditor extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(DbTranslationEditor.class.getName());
	
	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<String>();
	// a replacement for the given input
	private String origPixelPortion = null;
	private String relacementPixel = null;
	
	// set the engine name to find and replace
	private String engineToReplace = null;
	private String engineToFind = null;
	
	private boolean neededModifcation = false;
	
	@Override
	public void caseAConfiguration(AConfiguration node) {
        List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        for(PRoutine e : copy) {
        	String expression = e.toString();
        	LOGGER.info("Processing " + expression);
        	e.apply(this);
        	// if we ended up making the modificaiton
        	// the replacement string will not be null
        	if(this.relacementPixel != null && this.origPixelPortion != null) {
        		String newExpression = expression.replace(this.origPixelPortion, this.relacementPixel);
        		this.pixels.add(newExpression);
        		// now we need to null replacement
        		this.relacementPixel = null;
        		this.origPixelPortion = null;
        		
        		// set that we needed some kind of modication
        		this.neededModifcation = true;
        	} else {
        		this.pixels.add(expression);
        	}
        }
	}
	
	
	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);
        
        String reactorId = node.getId().toString().trim();
        if(reactorId.equals("Database")) {
        	// right now, the only input for database is in the curRow
        	String dbInput = node.getOpInput().toString().trim();
        	if(dbInput.equals(this.engineToFind)) {
        		// okay, it is a match
        		// we need to replace
        		this.origPixelPortion = node.toString();
        		this.relacementPixel = node.toString().replace(this.engineToFind, this.engineToReplace);
        	}
        }
	}
	
	public void setEngineToReplace(String engineToReplace) {
		this.engineToReplace = engineToReplace;
	}

	public void setEngineToFind(String engineToFind) {
		this.engineToFind = engineToFind;
	}
	
	public List<String> getPixels() {
		return pixels;
	}

	public boolean isNeededModifcation() {
		return neededModifcation;
	}


	public static void main(String[] args) {
		String expression = "CreateFrame(py); Database(Movie_RDBMS) | Select(Title, Title__Movie_Budget) | Import();";
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		DbTranslationEditor translation = new DbTranslationEditor();
		translation.setEngineToFind("Movie_RDBMS");
		translation.setEngineToReplace("MyMovie");

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(translation.pixels);
	}
	
}
