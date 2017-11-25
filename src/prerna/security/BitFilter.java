package prerna.security;
/*
 * Abstract base class for handling the filtering of bits.
 *
 * Written by Matthew Kwan - April 1997
 */

class BitFilter {
	public boolean		receive_bit (boolean bit) {
	    return (false);
	}

	public boolean		flush () {
	    return (false);
	}
}
