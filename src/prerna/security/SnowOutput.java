package prerna.security;
/*
 * Read bits and write them as characters.
 *
 * Written by Matthew Kwan - April 1997
 */

import java.io.IOException;
import java.io.OutputStream;

class SnowOutput extends BitFilter {
	private boolean		quiet_flag = false;
	private OutputStream	stream_out = null;
	private	int		bit_count;
	private	int		value;

	SnowOutput (boolean quiet, OutputStream stream) {
	    quiet_flag = quiet;
	    stream_out = stream;

	    bit_count = 0;
	    value = 0;
	}

	public boolean		receive_bit (boolean bit) {
	    value = (value << 1) | (bit ? 1 : 0);

	    if (++bit_count == 8) {
		try {
		    stream_out.write (value);
		} catch (IOException e) {
		    System.err.println ("Error: failed to write output.");
		    return (false);
		}

		value = 0;
		bit_count = 0;
	    }

	    return (true);
	}

	public boolean		flush () {
	    if (bit_count > 2 && !quiet_flag)
		System.err.println ("Warning: residual of " + bit_count
							+ " bits not output.");
	    return (true);
	}
}
