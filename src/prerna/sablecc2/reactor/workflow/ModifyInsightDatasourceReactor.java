package prerna.sablecc2.reactor.workflow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.translations.ReplaceDatasourceTranslation;

public class ModifyInsightDatasourceReactor extends AbstractReactor {

	public ModifyInsightDatasourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.OPTIONS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> replacementOptions = getOptions();

		List<String> recipe = this.insight.getPixelRecipe();
		StringBuilder b = new StringBuilder();
		for(String s : recipe) {
			b.append(s);
		}
		String fullRecipe = b.toString();
		
		/*
		 * Using a translation object to perform the replacement for me
		 * 
		 */
		
		ReplaceDatasourceTranslation translation = new ReplaceDatasourceTranslation();
		translation.setReplacements(replacementOptions);
		try {
			fullRecipe = PixelPreProcessor.preProcessPixel(fullRecipe, translation.encodedToOriginal);
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(fullRecipe.getBytes("UTF-8"))), fullRecipe.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}
		
		List<String> newRecipe = translation.getPixels();
		return new NounMetadata(newRecipe, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	/**
	 * Get the replacement information
	 * @return
	 */
	public List<Map<String, Object>> getOptions() {
		List<Map<String, Object>> ret = new Vector<Map<String, Object>>();
		
		GenRowStruct options = this.store.getNoun(this.keysToGet[0]);
		if(options != null && !options.isEmpty()) {
			int size = options.size();
			for(int i = 0; i < size; i++) {
				ret.add( (Map<String, Object>) options.get(i));
			}
			return ret;
		}
		
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			ret.add( (Map<String, Object>) this.curRow.get(i));
		}
		return ret;
	}

}
