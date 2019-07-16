package prerna.sablecc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.Hashtable;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class PKQLMetaTranslation extends Translation {

	public static void main(String[] args) {
		H2Frame frame = new H2Frame();
		PKQLRunner runner = new PKQLRunner();
		
		String expression = "data.import(api:csvFile.query([c:Nominated,c:Title,c:Genre],{'file':'C:\\workspace\\Semoss_Dev\\Movie_Data2016_08_29_14_12_11_0394.csv','Nominated':'STRING','Title':'STRING','Genre':'STRING'}));";
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;
		PKQLMetaTranslation translation = new PKQLMetaTranslation(frame, runner);

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		}
	}
	
	public PKQLMetaTranslation(PKQLRunner runner) { // Test Constructor
		super(runner);
	}
	
	/**
	 * Constructor that takes in the dataframe that it will perform its calculations off of and the runner that invoked the translation
	 * @param frame IDataMaker
	 * @param runner PKQLRunner: holds response from PKQL script and the status of whether the script errored or not
	 */
	public PKQLMetaTranslation(IDataMaker frame, PKQLRunner runner) {
		super(frame, runner);
	}
	
	public Hashtable <String, Object> deinitReactor(String myName, String input, String output, boolean put) {
		Hashtable <String, Object> thisReactorHash = reactorStack.lastElement();
		reactorStack.remove(thisReactorHash);
		IScriptReactor thisReactor = (IScriptReactor)thisReactorHash.get(myName);
		// this is still one level up
		///////////////////
		///////////////////
		// this is exactly the same as the method in Translation but we do not call thisReactor
//		thisReactor.process();
		Object value = 	thisReactor.getValue(input);
		System.out.println("Value is .. " + value);

		if(reactorStack.size() > 0) {
			reactorHash = reactorStack.lastElement();
			// also set the cur reactor
			String parent = (String)thisReactorHash.get("PARENT_NAME");

			// if the parent is not null
			if(parent != null && reactorHash.containsKey(parent)) {
				curReactor = (IScriptReactor)reactorHash.get(parent);
				if(put)
					curReactor.put(output, value);
				else
					curReactor.set(output, value);
			}
		}else if(reactorHash.size() > 0){ //if there is no parent reactor eg.: data.type
			//String self = (String) thisReactorHash.get("SELF");
			//if(self != null && reactorHash.containsKey(self)) {
			//curReactor = (IScriptReactor)reactorHash.get(self);
			if(put)
				curReactor.put(output, value);
			else
				curReactor.set(output, value);
			//}
		}
		return thisReactorHash;
	}
	
}
