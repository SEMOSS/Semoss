package prerna.sablecc2.reactor.workflow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.DatasourceTranslation;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightDatasourcesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> recipe = this.insight.getPixelRecipe();
		StringBuilder b = new StringBuilder();
		for(String s : recipe) {
			b.append(s);
		}
		String fullRecipe = b.toString();
		DatasourceTranslation translation = new DatasourceTranslation();
		try {
			fullRecipe = PixelPreProcessor.preProcessPixel(fullRecipe, new HashMap<String, String>());
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(fullRecipe.getBytes("UTF-8"))), fullRecipe.length())));
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}
		
		List<Map<String, Object>> sourcePixels = translation.getDatasourcePixels();
		return new NounMetadata(sourcePixels, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
