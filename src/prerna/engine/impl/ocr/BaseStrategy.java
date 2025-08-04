package prerna.engine.impl.ocr;

public enum BaseStrategy implements OCRStrategy {
	NO_IMAGE("no-image"),
	TESSERACT_1("tesseract-1"),
	TESSERACT_2("tesseract-2"),
	TESSERACT_3("tesseract-3"),
	INTEGRATED_PYMUPDF("integrated-pymupdf");
	
	private String strategyString;
	
	BaseStrategy(String string) {
		this.strategyString = string;
	}

	@Override
	public String getStrategyString() {
		return strategyString;
	}
	
}