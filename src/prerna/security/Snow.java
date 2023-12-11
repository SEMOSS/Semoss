package prerna.security;
/*
 * This class implements the main routine of SNOW.
 * SNOW is a command-line program for hiding and extracting messages
 * within the whitespace of text files.
 *
 * Usage: snow [-C][-Q][-S][-p passwd][-l line-len] [-f file | -m message]
 *
 *	-C : Use compression
 *	-Q : Be quiet
 *	-S : Calculate the space available in the file
 *	-l : Maximum line length allowable
 *	-p : Specify the password to encrypt the message
 *
 *	-f : Insert the message contained in the file
 *	-m : Insert the message given
 *
 * If the program is executed without either of the -f or -m options
 * then the program will attempt to extract a concealed message.
 * The output will go to outfile if specified, stdout otherwise.
 *
 * Written by Matthew Kwan - April 1997
 */

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

public class Snow {

	private static final Logger logger = LogManager.getLogger(Snow.class);

	private static final String STACKTRACE = "StackTrace: ";

	private  BufferedReader	stream_in;
	private  OutputStream	stream_out;
	private InputStream	stream_message = null;

	private boolean		compress_flag = false;
	private boolean		quiet_flag = false;
	private boolean		space_flag = false;
	private String		passwd_string = null;
	private int		line_length = 80;


		// Parse the command-line arguments.
	private  boolean		parse_args (String argv[]) {
	    int		optind;

	    for (optind = 0; optind < argv.length
				&& argv[optind].charAt(0) == '-'; optind++) {
		String	optarg;

		if (argv[optind].length() < 2)
		    return (false);

		switch (argv[optind].charAt(1)) {
		    case 'C':
			compress_flag = true;
			break;
		    case 'Q':
			quiet_flag = true;
			break;
		    case 'S':
			space_flag = true;
			break;
		    case 'f':
			if (argv[optind].length() > 2)
			    optarg = argv[optind].substring(2);
			else if (++optind == argv.length)
			    return (false);
			else
			    optarg = argv[optind];

				if (stream_message != null) {
					logger.error("Multiple message inputs defined.");
					return (false);
				}

			try {
			    stream_message = new FileInputStream (Utility.normalizePath(optarg));
			} catch (FileNotFoundException e) {
				logger.error("No such file: " + Utility.cleanLogString(optarg));
			    return (false);
			}
			break;
		    case 'l':
			if (argv[optind].length() > 2)
			    optarg = argv[optind].substring(2);
			else if (++optind == argv.length)
			    return (false);
			else
			    optarg = argv[optind];

			try {
			    line_length = Integer.parseInt (optarg);
				} catch (NumberFormatException e) {
					logger.error("Illegal line length: " + Utility.cleanLogString(optarg));
					return (false);
				}
			break;
		    case 'm':
			if (argv[optind].length() > 2)
			    optarg = argv[optind].substring(2);
			else if (++optind == argv.length)
			    return (false);
			else
			    optarg = argv[optind];

				if (stream_message != null) {
					logger.error("Multiple message inputs defined.");
					return (false);
				}

			stream_message = new
				ByteArrayInputStream (optarg.getBytes());

			break;
			case 'p':
				if (argv[optind].length() > 2)
					optarg = argv[optind].substring(2);
				else if (++optind == argv.length)
					return (false);
				else
					optarg = argv[optind];

				passwd_string = optarg;
				break;
			default:
				logger.error("Illegal option: " + Utility.cleanLogString(argv[optind]));
				return (false);
			}
		}

	    if (optind < argv.length - 2)
		return (false);

	    if (optind < argv.length) {
		try {
			// this is the input file it parses
		    stream_in = new BufferedReader (new
						FileReader (Utility.normalizePath(argv[optind])));
		} catch (FileNotFoundException e) {
				logger.error("No such file: " + Utility.cleanLogString(argv[optind]));
				return (false);
			}
		} else
			stream_in = new BufferedReader(new InputStreamReader(System.in));

		if (!space_flag && optind + 1 < argv.length) {
			try {
				// this is the output file it writes to
				stream_out = new FileOutputStream(Utility.normalizePath(argv[optind + 1]));

			} catch (IOException e) {
				logger.error("Could not open file for writing: " + Utility.cleanLogString(argv[optind + 1]));
				return (false);
			}
		} else
			stream_out = new ByteArrayOutputStream();

		return (true);
	}

		// Send the contents of the message stream to the bit filter.
	private boolean message_encode(BitFilter bf) {
		try {
			int v;

			while ((v = stream_message.read()) != -1) {
				for (int i = 0; i < 8; i++) {
					if (!bf.receive_bit((v & (128 >> i)) != 0))
						return (false);
				}
			}
		} catch (IOException e) {
			logger.error("Failed to read from message stream.");
			return (false);
		}

		return (bf.flush());
	}
	
	
	
	public String runSnow(String [] argv)
	{
		parse_args (argv);
	    if (space_flag) {
		SnowEncode	se = new SnowEncode (true, quiet_flag, null,
						stream_in, null, line_length);

		se.space_calculate ();
	    } else if (stream_message != null) {
		PrintWriter	pw = new PrintWriter (stream_out);
		BitFilter	bf = new SnowEncode (true, quiet_flag, null,
						stream_in, pw, line_length);

		if (passwd_string != null)
		    bf = new SnowEncrypt (true, quiet_flag, bf, passwd_string);
		if (compress_flag)
		    bf = new SnowCompress (true, quiet_flag, bf);
		if (!message_encode (bf))
		    //System.exit (1);

		pw.close();

		} else {
			BitFilter bf = new SnowOutput(quiet_flag, stream_out);
			SnowEncode se;

		if (compress_flag)
		    bf = new SnowCompress (false, quiet_flag, bf);
		if (passwd_string != null)
		    bf = new SnowEncrypt (false, quiet_flag, bf, passwd_string);

		se = new SnowEncode (false, quiet_flag, bf, stream_in, null,
								line_length);
		if (!se.decode ())
		    //System.exit (1);

		try {
		    stream_out.close ();
		} catch (IOException e) {
			logger.error("Problem closing output file.");
		    //System.exit (1);
		}
	    }

		try {
			stream_in.close();
		} catch (IOException e) {
			logger.error("Problem closing input file.");
			// System.exit (1);
		}

	    if(stream_out instanceof ByteArrayOutputStream)
	    {
	    	// print it out
	    	String output = stream_out.toString();
	    	return output;
	    }
	    return null;
	}
	

		// Entry point to the program.
//	public static void	main (String argv[]) {
//		Snow snow = new Snow();
//		
//		if (!snow.parse_args(argv)) {
//			logger.error("Usage: Snow.class [-C][-Q][-S]");
//			logger.error("[-p passwd][-l line-len]");
//			logger.error(" [-f file][-m message]");
//			logger.error("\t\t\t\t\t[infile [outfile]]");
//			System.exit(1);
//		}
//
//	    if (snow.space_flag) {
//		SnowEncode	se = new SnowEncode (true, snow.quiet_flag, null,
//				snow.stream_in, null, snow.line_length);
//
//			se.space_calculate();
//		} else if (snow.stream_message != null) {
//			PrintWriter pw = new PrintWriter(snow.stream_out);
//			BitFilter bf = new SnowEncode(true, snow.quiet_flag, null, snow.stream_in, pw, snow.line_length);
//
//			if (snow.passwd_string != null)
//				bf = new SnowEncrypt(true, snow.quiet_flag, bf, snow.passwd_string);
//			if (snow.compress_flag)
//				bf = new SnowCompress(true, snow.quiet_flag, bf);
//			if (!snow.message_encode(bf))
//				System.exit(1);
//
//			pw.close ();
//		} else {
//			BitFilter bf = new SnowOutput(snow.quiet_flag, snow.stream_out);
//			SnowEncode se;
//
//			if (snow.compress_flag)
//				bf = new SnowCompress(false, snow.quiet_flag, bf);
//			if (snow.passwd_string != null)
//				bf = new SnowEncrypt(false, snow.quiet_flag, bf, snow.passwd_string);
//
//			se = new SnowEncode(false, snow.quiet_flag, bf, snow.stream_in, null, snow.line_length);
//			if (!se.decode())
//				System.exit(1);
//
//			try {
//				snow.stream_out.close();
//			} catch (IOException e) {
//				logger.error("Problem closing output file.");
//				System.exit(1);
//			}
//		}
//
//		try {
//			snow.stream_in.close();
//		} catch (IOException e) {
//			logger.error("Problem closing input file.");
//			System.exit(1);
//		}
//	}
}
