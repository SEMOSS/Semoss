package prerna.testing;

public class PixelChain {

	private Class<?> c;
	private Object[] args;
	private String rawPixel;
	private boolean isRawPixel;

	public PixelChain(Class<?> c, Object... args) {
		this.c = c;
		this.args = args;
		this.isRawPixel = false;
	}

	public PixelChain(String rawPixel) {
		this.rawPixel = rawPixel;
		this.isRawPixel = true;
	}

	public Class<?> getC() {
		return c;
	}

	public void setC(Class<?> c) {
		this.c = c;
	}

	public Object[] getArgs() {
		return args;
	}

	public void setArgs(Object[] args) {
		this.args = args;
	}

	public String getRawPixel() {
		return rawPixel;
	}

	public void setRawPixel(String rawPixel) {
		this.rawPixel = rawPixel;
	}

	public boolean isRawPixel() {
		return isRawPixel;
	}

	public void setRawPixel(boolean isRawPixel) {
		this.isRawPixel = isRawPixel;
	}

}
