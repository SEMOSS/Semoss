package prerna.ds;

public class EmptyIteratorException extends IllegalArgumentException {

	public EmptyIteratorException() {
		super();
	}
	
	public EmptyIteratorException(String s) {
        super(s);
    }
}
