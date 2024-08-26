///* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
// * This file is part of SableCC.                             *
// * See the file "LICENSE" for copyright information and the  *
// * terms and conditions for copying, distribution and        *
// * modification of SableCC.                                  *
// * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
//
//package org.sablecc.sablecc;
//
//import java.io.File;
//import java.io.FileReader;
//import java.io.PushbackReader;
//import java.util.Vector;
//
//import org.sablecc.sablecc.lexer.Lexer;
//import org.sablecc.sablecc.node.AGrammar;
//import org.sablecc.sablecc.node.Start;
//import org.sablecc.sablecc.parser.Parser;
//
//@SuppressWarnings({"rawtypes","unchecked"})
//public class SableCC
//{
//	private static boolean processInlining = true;
//	static int inliningMaxAlts = 20;
//	private static boolean prettyPrinting = false;
//
//	private static final String OPT_LICENSE = "--license";
//	private static final String OPT_D = "-d";
//	private static final String OPT_NO_INLINE = "--no-inline";
//	private static final String OPT_INLINE_MAX_ALTS = "--inline-max-alts";
//	private static final String OPT_PRETTY_PRINT = "--pretty-print";
//
//	private static void displayCopyright()
//	{
//		System.out.println();
//		System.out.println("SableCC version " + Version.VERSION);
//		System.out.println("Copyright (C) 1997-2012 Etienne M. Gagnon <egagnon@j-meg.com> and");
//		System.out.println("others.  All rights reserved.");
//		System.out.println();
//		System.out.println("This software comes with ABSOLUTELY NO WARRANTY.  This is free software,");
//		System.out.println("and you are welcome to redistribute it under certain conditions.");
//		System.out.println();
//		System.out.println("Type 'sablecc -license' to view");
//		System.out.println("the complete copyright notice and license.");
//		System.out.println();
//	}
//
//	private static void displayUsage()
//	{
//		System.out.println("Usage:");
//		System.out.println("  sablecc [" +
//				OPT_D + " destination] [" +
//				OPT_NO_INLINE + "] [" +
//				OPT_INLINE_MAX_ALTS + " number] [" +
//				OPT_PRETTY_PRINT + "] filename [filename]...");
//		System.out.println("  sablecc " + OPT_LICENSE);
//	}
//
//	public static void main(String[] arguments)
//	{
//		String d_option = null;
//		Vector filename = new Vector();
//		arguments = new String[]{"C:/workspace/Semoss/src/postfix2.grammar"};
//
//		if(arguments.length == 0)
//		{
//			displayCopyright();
//			displayUsage();
//			System.exit(1);
//		}
//
//		if((arguments.length == 1) && (arguments[0].equals(OPT_LICENSE)))
//		{
//			new DisplayLicense();
//			System.exit(0);
//		}
//
//		displayCopyright();
//
//		{
//			int arg = 0;
//			while(arg < arguments.length)
//			{
//				if(arguments[arg].equals(OPT_D))
//				{
//					if((d_option == null) && (++arg < arguments.length))
//					{
//						d_option = arguments[arg];
//					}
//					else
//					{
//						displayUsage();
//						System.exit(1);
//					}
//				}
//				else if(arguments[arg].equals(OPT_NO_INLINE))
//				{
//					processInlining = false;
//				}
//				/* A production is not inlined if it has more than
//				 * inliningMaxAlts alternatives.  The default value is 20. */
//				else if(arguments[arg].equals(OPT_INLINE_MAX_ALTS))
//				{
//					try
//					{
//						inliningMaxAlts = Integer.parseInt(arguments[++arg]);
//					}
//					catch(Exception e)
//					{
//						displayUsage();
//						System.exit(1);
//					}
//				}
//				/*
//          if prettyprint flag is set to true, only the transformed
//          grammar is printed on standard output
//				 */
//				else if(arguments[arg].equals(OPT_PRETTY_PRINT))
//				{
//					prettyPrinting = true;
//				}
//				else
//				{
//					filename.addElement(arguments[arg]);
//				}
//				arg++;
//			}
//
//			if(filename.size() == 0)
//			{
//				displayUsage();
//				System.exit(1);
//			}
//		}
//
//		try
//		{
//			for(int i=0; i<filename.size(); i++)
//			{
//				processGrammar((String)filename.elementAt(i), d_option);
//			}
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//			System.exit(1);
//		}
//		System.exit(0);
//	}
//
//	/**
//	 * The main method for processing grammar file and generating the parser/lexer.
//	 * @param grammar input grammar file name
//	 * @param destDir output directory name
//	 */
//	public static void processGrammar(String grammar, String destDir) throws Exception
//	{
//		File in;
//		File dir;
//
//		in = new File(grammar);
//		in = new File(in.getAbsolutePath());
//
//		if(destDir == null)
//		{
//			dir = new File(in.getParent());
//		}
//		else
//		{
//			dir = new File(destDir);
//			dir = new File(dir.getAbsolutePath());
//		}
//
//		processGrammar(in, dir);
//	}
//
//	/**
//	 * The main method for processing grammar file and generating the parser/lexer.
//	 * @param in input grammar file
//	 * @param dir output directory
//	 */
//	public static void processGrammar(File in,  File dir) throws Exception
//	{
//		if(!in.exists())
//		{
//			System.out.println("ERROR: grammar file "+in.getName()+" does not exist.");
//			System.exit(1);
//		}
//		if(!dir.exists())
//		{
//			System.out.println("ERROR: destination directory "+dir.getName()+" does not exist.");
//			System.exit(1);
//		}
//
//		// re-initialize all static structures in the engine
//		LR0Collection.reinit();
//		Symbol.reinit();
//		Production.reinit();
//		Grammar.reinit();
//
//		System.out.println("\n -- Generating parser for "+in.getName()+" in "+dir.getPath());
//
//		FileReader temp = new FileReader(in);
//
//		// Build the AST
//		Start tree = new Parser(new Lexer(new PushbackReader(
//				temp = new FileReader(in), 1000))).parse();
//
//		temp.close();
//
//		boolean hasTransformations = false;
//
//		if( ((AGrammar)tree.getPGrammar()).getAst() == null )
//		{
//			System.out.println("Adding productions and alternative of section AST.");
//			//AddAstProductions astProductions = new AddAstProductions();
//			tree.apply(new AddAstProductions());
//		}
//		else
//		{
//			hasTransformations = true;
//		}
//
//		System.out.println("Verifying identifiers.");
//		ResolveIds ids = new ResolveIds(dir);
//		tree.apply(ids);
//
//		System.out.println("Verifying ast identifiers.");
//		ResolveAstIds ast_ids = new ResolveAstIds(ids);
//		tree.apply(ast_ids);
//
//		System.out.println("Adding empty productions and empty alternative transformation if necessary.");
//		tree.apply( new AddEventualEmptyTransformationToProductions(ids, ast_ids) );
//
//		System.out.println("Adding productions and alternative transformation if necessary.");
//		AddProdTransformAndAltTransform adds = new AddProdTransformAndAltTransform();
//		tree.apply(adds);
//		/*
//    System.out.println("Replacing AST + operator by * and removing ? operator if necessary");
//    tree.apply( new AstTransformations() );
//		 */
//		System.out.println("computing alternative symbol table identifiers.");
//		ResolveAltIds alt_ids = new ResolveAltIds(ids);
//		tree.apply(alt_ids);
//
//		System.out.println("Verifying production transform identifiers.");
//		ResolveProdTransformIds ptransform_ids = new ResolveProdTransformIds(ast_ids);
//		tree.apply(ptransform_ids);
//
//		System.out.println("Verifying ast alternatives transform identifiers.");
//		ResolveTransformIds transform_ids = new ResolveTransformIds(ast_ids, alt_ids, ptransform_ids);
//		tree.apply(transform_ids);
//
//		System.out.println("Generating token classes.");
//		tree.apply(new GenTokens(ids));
//
//		System.out.println("Generating production classes.");
//		tree.apply(new GenProds(ast_ids));
//
//		System.out.println("Generating alternative classes.");
//		tree.apply(new GenAlts(ast_ids));
//
//		System.out.println("Generating analysis classes.");
//		tree.apply(new GenAnalyses(ast_ids));
//
//		System.out.println("Generating utility classes.");
//		tree.apply(new GenUtils(ast_ids));
//
//		try
//		{
//			System.out.println("Generating the lexer.");
//			tree.apply(new GenLexer(ids));
//		}
//		catch(Exception e)
//		{
//			System.out.println(e.getMessage());
//			throw e;
//		}
//
//		try
//		{
//			System.out.println("Generating the parser.");
//			tree.apply(new GenParser(ids, alt_ids, transform_ids, ast_ids.getFirstAstProduction(), processInlining, prettyPrinting, hasTransformations) );
//		}
//		catch(Exception e)
//		{
//			System.out.println(e.getMessage());
//			throw e;
//		}
//	}
//}
