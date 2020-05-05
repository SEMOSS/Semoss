package prerna.security;
/*
 * Whitespace encoding routines for SNOW.
 *
 * Written by Matthew Kwan - April 1997
 */

import java.io.*;

class SnowEncode extends BitFilter {
	private boolean		encode_flag = false;
	private boolean		quiet_flag = false;
	private BitFilter	next_filter = null;

	private BufferedReader	stream_in = null;
	private PrintWriter	stream_out = null;
	private int		line_length;

	private int		bit_count;
	private int		value;
	private char		buffer[];
	private boolean		buffer_loaded;
	private int		buffer_length;
	private int		buffer_column;
	private boolean		first_tab;
	private boolean		needs_tab;
	private int		bits_used;
	private int		bits_available;
	private int		lines_extra;

	SnowEncode (
	    boolean		encode,
	    boolean		quiet,
	    BitFilter		output,
	    BufferedReader	in,
	    PrintWriter		out,
	    int			linelen
	) {
	    encode_flag = encode;
	    quiet_flag = quiet;
	    next_filter = output;
	    stream_in = in;
	    stream_out = out;
	    line_length = linelen;

	    bit_count = 0;
	    value = 0;
	    buffer = new char[4096];
	    buffer_loaded = false;
	    buffer_length = 0;
	    buffer_column = 0;
	    first_tab = false;
	    bits_used = 0;
	    bits_available = 0;
	    lines_extra = 0;
	}

		// Return the next tab position.
	private int		tabpos (int n) {
	    return ((n + 8) & ~7);
	}

		// Read a line of text, like fgets, but strip off
		// trailing whitespace.
	private int		wsgets (BufferedReader ins, char buf[]) {
	    String	s;
	    int		i;

	    try {
		if ((s = ins.readLine ()) == null)
		    return (-1);
	    } catch (IOException e) {
		return (-1);
	    }

	    for (i = s.length(); i > 0; i--)
		if (s.charAt (i - 1) > ' ')
		    break;

	    if (i == 0)
		return (0);

	    s.getChars (0, i, buf, 0);
	    return (i);
	}

		// Write a line of text, adding a newline.
		// Return false if the write fails.
	private boolean		wsputs (PrintWriter outs, char s[], int len) {
	    outs.write (s, 0, len);
	    outs.println ();

	    return (!outs.checkError ());
	}

		// Calculate, approximately, how many bits can be stored
		// in a line of length "len".
	private void		whitespace_storage (int len, int range[]) {
	    if (len > line_length - 2)
		return;

	    if (len / 8 == line_length / 8) {
		range[1] += 3;
		return;
	    }

	    if ((len & 7) > 0) {
		range[1] += 3;
		len = tabpos (len);
	    }
	    if ((line_length & 7) > 0)
		range[1] += 3;

	    int		n = ((line_length - len) / 8) * 3;

	    range[1] += n;
	    range[0] += n;
	}

		// Load the buffer.
		// If there is no text to read, make it empty.
	private void		buffer_load (BufferedReader ins) {
	    if ((buffer_length = wsgets (ins, buffer)) < 0) {
		buffer_length = 0;
		lines_extra++;
	    }

	    buffer_column = 0;
	    for (int i=0; i<buffer_length; i++)
		if (buffer[i] == '\t')
		    buffer_column = tabpos (buffer_column);
		else
		    buffer_column++;

	    buffer_loaded = true;
	    needs_tab = false;
	}

		// Append whitespace to the loaded buffer, if there is room.
	private boolean		append_whitespace (int nsp) {
	    int		col = buffer_column;

	    if (needs_tab)
		col = tabpos (col);

	    if (nsp == 0)
		col = tabpos (col);
	    else
		col += nsp;

	    if (col >= line_length)
		return (false);

	    if (needs_tab) {
		buffer[buffer_length++] = '\t';
		buffer_column = tabpos (buffer_column);
	    }

	    if (nsp == 0) {
		buffer[buffer_length++] = '\t';
		buffer_column = tabpos (buffer_column);
		needs_tab = false;
	    } else {
		for (int i=0; i<nsp; i++) {
		    buffer[buffer_length++] = ' ';
		    buffer_column++;
		}

		needs_tab = true;
	    }

	    return (true);
	}

		// Hide a 3-bit value in the output stream.
	private boolean		write_value (int val) {
	    if (!buffer_loaded)
		buffer_load (stream_in);

	    if (!first_tab) {		// Tab shows start of data
		while (tabpos (buffer_column) >= line_length) {
		    if (!wsputs (stream_out, buffer, buffer_length))
			return (false);
		    buffer_load (stream_in);
		}

		buffer[buffer_length++] = '\t';
		buffer_column = tabpos (buffer_column);
		first_tab = true;
	    }

				// Reverse the bit ordering
	    int		nspc = ((val & 1) << 2) | (val & 2) | ((val & 4) >> 2);

	    while (!append_whitespace (nspc)) {
		if (!wsputs (stream_out, buffer, buffer_length))
		    return (false);
		buffer_load (stream_in);
	    }

	    if (lines_extra == 0)
		bits_available += 3;

	    return (true);
	}

	public boolean		receive_bit (boolean bit) {
	    if (encode_flag) {
		value = (value << 1) | (bit ? 1 : 0);
		bits_used++;

		if (++bit_count == 3) {
		    if (!write_value (value))
			return (false);

		    value = 0;
		    bit_count = 0;
		}

		return (true);
	    } else
		return (false);

	}

	public boolean		flush () {
	    if (!encode_flag)
		return (next_filter.flush ());

	    if (bit_count > 0) {
		while (bit_count < 3) {		// Pad to 3 bits.
		    value <<= 1;
		    bit_count++;
		}

		if (!write_value (value))
		    return (false);
	    }

	    if (buffer_loaded) {
		if (!wsputs (stream_out, buffer, buffer_length))
		    return (false);
		buffer_loaded = false;
		buffer_length = 0;
		buffer_column = 0;
	    }

	    int		n;
	    int		storage[] = new int[2];

	    storage[0] = storage[1] = 0;
	    while ((n = wsgets (stream_in, buffer)) >= 0) {
		whitespace_storage (n, storage);
		if (!wsputs (stream_out, buffer, n))
		    return (false);
	    }

	    bits_available += (storage[0] + storage[1]) / 2;

	    if (!quiet_flag) {
		double	usage = 100.0 * bits_used / bits_available;

		usage = Math.rint (usage * 100.0) / 100.0;	// Rounding

		if (lines_extra > 0) {
		    /*System.err.println (
			"Message exceeded available space by approximately "
						+ (usage - 100.0) + "%.");
		    System.err.println ("An extra  " + lines_extra
						+ " lines were added.");*/
		} else {
		  /*  System.err.println ("Message used approximately "
					+ usage + "% of available space.");*/
		}
	    }

	    return (true);
	}

		// Decode the space count into actual bits.
	private boolean		decode_bits (int spc) {
	    if (spc > 7) {
		//System.err.println ("Illegal encoding of " + spc + " spaces.");
		return (false);
	    }

	    if (!next_filter.receive_bit ((spc & 1) != 0))
		return (false);
	    if (!next_filter.receive_bit ((spc & 2) != 0))
		return (false);
	    if (!next_filter.receive_bit ((spc & 4) != 0))
		return (false);

	    return (true);
	}

		// Decode the whitespace contained in the internal buffer.
	private boolean		decode_whitespace (int idx_lo, int idx_hi) {
	    int		spc = 0;

	    for (int i=idx_lo; i<idx_hi; i++) {
		if (buffer[i] == '\t') {
		    if (!decode_bits (spc))
			return (false);
		    spc = 0;
		} else if (buffer[i] == ' ')
		    spc++;
	    }

	    if (spc > 0 && !decode_bits (spc))
		return (false);

	    return (true);
	}

		// Read a line of text, like fgets, but only return the
		// trailing whitespace.
	private int	read_whitespace (BufferedReader ins, char buf[]) {
	    String	s;
	    int		i, len;

	    try {
		if ((s = ins.readLine ()) == null)
		    return (-1);
	    } catch (IOException e) {
		return (-1);
	    }

	    len = s.length ();
	    while (len > 0 && s.charAt (len - 1) == '\n')
		len--;

	    for (i = len; i > 0; i--)
		if (s.charAt (i - 1) > ' ')
		    break;

	    if (i == len)
		return (0);

	    s.getChars (i, len, buf, 0);
	    return (len - i);
	}

		// Process the input stream to decode the message.
	public boolean		decode () {
	    boolean	start_tab_found = false;
	    int		len;
 
	    while ((len = read_whitespace (stream_in, buffer)) >= 0) {
		int	start_idx = 0;

		if (len == 0)
		    continue;
		if (!start_tab_found && buffer[0] == ' ')
		    continue;

		if (!start_tab_found && buffer[0] == '\t') {
		    start_tab_found = true;
		    start_idx = 1;
		    if (len == 1)
			continue;
		}

		if (!decode_whitespace (start_idx, len))
		    return (false);
	    }

	    return (next_filter.flush ());
	}

		// Calculate the storage space of the input file.
	public void		space_calculate () {
	    int		n;
	    int		storage[] = new int[2];

	    storage[0] = storage[1] = 0;
	    while ((n = wsgets (stream_in , buffer)) >= 0)
		whitespace_storage (n, storage);

	    if (storage[0] > 0) {	// Allow for initial tab.
		storage[0]--;
		storage[1]--;
	    }

	    if (storage[0] == storage[1]) {
	//	System.out.println ("File has storage capacity of "
	//		+ storage[0] + " bits (" + storage[0] / 8 + " bytes)");
	    } else {
	//	System.out.println ("File has storage capacity of between "
	//		+ storage[0] + " and " + storage[1] + " bits.");
	//	System.out.println ("Approximately "
	//			+ (storage[0] + storage[1]) / 16 + " bytes.");
	    }
	}
}
