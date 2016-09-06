package prerna.sablecc.meta;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import prerna.ui.components.playsheets.datamakers.PKQLTransformation;

public abstract class AbstractPkqlMetadata implements IPkqlMetadata{

	protected String pkqlStr;
	protected PKQLTransformation trans;

	@Override
	public void setPkqlStr(String pkqlStr) {
		this.pkqlStr = pkqlStr;
	}
	
	@Override
	public String getPkqlStr() {
		return this.pkqlStr;
	}
	
	@Override
	public void setInvokingPkqlTransformation(PKQLTransformation trans) {
		this.trans = trans;
	}

	@Override
	public PKQLTransformation getInvokingPkqlTransformation() {
		return this.trans;
	}
	
	public String generateExplaination(String template, Map<String, Object> values) {
		String msg = "";
		StringWriter writer = new StringWriter();
		MustacheFactory mf = new DefaultMustacheFactory();
		try {
			Mustache mustache = mf.compile(new StringReader(template), "");
			mustache.execute(writer, values);
			writer.flush();
			msg = writer.toString();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return msg;
	}
}
