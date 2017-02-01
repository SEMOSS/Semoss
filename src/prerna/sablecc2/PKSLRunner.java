package prerna.sablecc2;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;

import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class PKSLRunner {

	/**
	 * Runs a given pksl expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pkql expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pkql expression on
	 */
	
	private Translation translation;
	private IDataMaker dataMaker;
	private String insightId;
	
	public void runPKSL(String expression, IDataMaker frame) {
		
		this.dataMaker = frame;
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;
		if(translation == null){
			translation = new Translation(frame, this);
		}

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public void setInsightId(String insightId) {
		this.insightId = insightId;
	}
	public String getInsightId() {
		return this.insightId;
	}

	public IDataMaker getDataFrame() {
		return this.dataMaker;
	}
}
