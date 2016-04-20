package prerna.sablecc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;

import org.apache.commons.lang.StringEscapeUtils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;

public class PKQLRunner {
	
	enum Status {SUCCESS, ERROR}
	
	private String currentStatus = "error";
	private Object response = "Invalid PKQL Statement";

	public HashMap<String, Object> runPKQL(String expression, ITableDataFrame f) {
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;

		try {
			tree = p.parse();
			// Apply the translation.
			tree.apply(new Translation2(f, this));

		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}

		HashMap<String, Object> result = new HashMap<String, Object>();
		result.put("result", StringEscapeUtils.escapeHtml(response + ""));
		result.put("status", currentStatus);
		return result;
	}

	public void setResponse(Object response) {
		this.response = response;
	}
	
	public void setStatus(String currentStatus) {
		this.currentStatus = currentStatus;
	}
}
