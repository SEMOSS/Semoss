package prerna.poi.main.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class NewLinePredictor {

	private static final char CR = '\r';
	private static final char LF = '\n';

	public static char[] predict(String fileName) {    
		Reader freader = null;
		Reader reader = null;
		try {
			freader = new FileReader(fileName);
			reader = new BufferedReader(freader);
			char[] result = predict(reader);
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(freader != null) {
				try {
					freader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return new char[]{CR, LF};
	}

	private static char[] predict(Reader reader) throws IOException {
		int c;
		while ((c = reader.read()) != -1) {
			switch(c) {        
			case LF: return new char[]{LF};
			case CR: {
				if (reader.read() == LF) {
					return new char[]{CR, LF};
				}
				return new char[]{CR};
			}
			default: continue;
			}
		}
		return new char[]{CR, LF};
	}

}
