package prerna.security;
/*
 * Encryption routines for SNOW.
 * Uses the ICE encryption algorithm in 1-bit cipher-feedback (CFB) mode.
 *
 * Written by Matthew Kwan - April 1997
 */

class SnowEncrypt extends BitFilter {
	private IceKey		key = null;
	private byte		iv_block[];
	private boolean		encode_flag = false;
	private boolean		quiet_flag = false;
	private BitFilter	next_filter = null;

	SnowEncrypt (
	    boolean	encode,
	    boolean	quiet,
	    BitFilter	output,
	    String	passwd
	) {
	    encode_flag = encode;
	    quiet_flag = quiet;
	    set_key (passwd);
	    next_filter = output;
	}

	private void	set_key (String passwd) {
	    byte	passbytes[] = passwd.getBytes();
	    int		level = (passbytes.length * 7 + 63) / 64;
	    byte	buf[];
	    int		pass_idx, i;

	    if (level == 0) {
		if (!quiet_flag)
		    System.err.println (
				"Warning: an empty password is being used.");
		level = 1;
	    } else if (level > 128) {
		if (!quiet_flag)
		    System.err.println (
				"Warning: password truncated to 1170 chars.");
		level = 128;
	    }

	    key = new IceKey (level);

	    buf = new byte[level * 8];
	    pass_idx = i = 0;
	    while (pass_idx < passbytes.length) {
		int	c = passbytes[pass_idx] & 0x7f;
		int	idx = i / 8;
		int	offset = i & 7;

		if (offset == 0) {
		    buf[idx] = (byte) (c << 1);
		} else if (offset == 1) {
		    buf[idx] |= c;
		} else {
		    buf[idx] |= (c >>> (offset - 1));
		    buf[idx + 1] = (byte) (c << (9 - offset));
		}

		i += 7;
		pass_idx++;

		if (i > 8184)
		    break;
	    }

	    key.set (buf);

			// Set the initialization vector to the key
			// encrypted with itself.
	    iv_block = new byte[8];
	    key.encrypt (buf, iv_block);
	}

	public boolean		receive_bit (boolean bit) {
	    if (key == null)
		return (next_filter.receive_bit (bit));

	    if (encode_flag) {
		byte	buf[] = new byte[8];

		key.encrypt (iv_block, buf);
		if ((buf[0] & 128) != 0)
		    bit = !bit;

			// Shift the IV block one bit left
		for (int i=0; i<8; i++) {
		    iv_block[i] <<= 1;
		    if (i < 7 && (iv_block[i+1] & 128) != 0)
			iv_block[i] |= 1;
		}
		iv_block[7] |= bit ? 1 : 0;

		return (next_filter.receive_bit (bit));
	    } else {
		boolean	nbit;
		byte	buf[] = new byte[8];

		key.encrypt (iv_block, buf);
		if ((buf[0] & 128) != 0)
		    nbit = !bit;
		else
		    nbit = bit;

			// Shift the IV block one bit left
		for (int i=0; i<8; i++) {
		    iv_block[i] <<= 1;
		    if (i < 7 && (iv_block[i+1] & 128) != 0)
			iv_block[i] |= 1;
		}
		iv_block[7] |= bit ? 1 : 0;

		return (next_filter.receive_bit (nbit));
	    }

	}

	public boolean		flush () {
	    return (next_filter.flush ());
	}
}
