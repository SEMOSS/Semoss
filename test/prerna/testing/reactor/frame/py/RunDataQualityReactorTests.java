package prerna.testing.reactor.frame.py;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.reactor.database.upload.PredictDataTypesReactor;
import prerna.reactor.frame.py.RunDataQualityReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.reactor.imports.ImportTestUtility;

public class RunDataQualityReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	public void testMoviesDataQuality() {
		String frameType = "Py";
		String frameAlias = "PYFRAME_8afc5e4d_21b7_49e5_bc78_3f475d31932f";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createMovieFrame(frameType, frameAlias, override);
		//PYFRAME_8afc5e4d_21b7_49e5_bc78_3f475d31932f | RunDataQuality ( rule = [ "Name Format" ] , column = [ "Director" ] , options = [ "first last" ] , inputTable = [ "dataQualityTable_1727807678928" ] ) ;
		PandasFrame newFrame = runRunDataQualityPixel("Name Format", "Director", "first last", "dataQualityTable_1727807678928");
		String[] headers = newFrame.getColumnHeaders();
		System.out.println(Arrays.toString(headers));
		Assert.assertArrayEquals(new String[] { "Columns", "Description", "Errors", "Rules", "toColor", "Total", "Valid"}, headers);
		// Mimicking FE Color by Value (CBV)
		String cbvPixel = "cbv_1727807710712 = Frame ( PYFRAME_8afc5e4d_21b7_49e5_bc78_3f475d31932f ) | ImplicitFilterOverride ( true ) | Select ( Director ) | Group ( Director ) | Filter ( ( Director == [ \"Alejandro_González_Iñárritu\" , \"Stephen_Gaghan\" , \"Lee_Daniels\" , \"Francis_Lawrence\" , \"Paul_McGuigan\" , \"David_Dobkin\" , \"Marc_Forster\" , \"Denzel_Washington\" , \"Gábor_Csupó\" , \"Lisa_Cholodenko\" , \"Adam_McKay\" , \"Tim_Story\" , \"Stephen_Frears\" , \"Steven_Soderbergh\" , \"Derek_Cianfrance\" , \"James_Mangold\" , \"Bruce_A._Evans\" , \"Peter_Cattaneo\" , \"Sylvain_White\" , \"Steven_Spielberg\" , \"Oliver_Stone\" , \"John_Polson\" , \"Bob_Dolman\" , \"Michael_Hoffman\" , \"Roger_Michell\" , \"M._Night_Shyamalan\" , \"Daniel_Barnz\" , \"Joe_Johnston\" , \"Joel_&_Ethan_Coen\" , \"Stephen_Kay\" , \"Gurinder_Chadha\" , \"Michel_Hazanavicius\" , \"Joel_Schumacher\" , \"Patrick_Tatopoulos\" , \"D.J._Caruso\" , \"Frank_Oz\" , \"Walter_Salles\" , \"Tyler_Perry\" , \"Jonathan_Liebesman\" , \"James_Wong\" , \"Sofia_Coppola\" , \"James_Ivory\" , \"Michael_Mann\" , \"Neil_LaBute\" , \"Alexander_Payne\" , \"Antony_Hoffman\" , \"Michael_Haneke\" , \"Thomas_McCarthy\" , \"Antoine_Fuqua\" , \"Hugh_Johnson\" , \"Karyn_Kusama\" , \"Peter_Berg\" , \"Benh_Zeitlin\" , \"Brian_Helgeland\" , \"Peter_Jackson\" , \"Peter_Chelsom\" , \"Benny_Boom\" , \"Michael_Apted\" , \"Cameron_Crowe\" , \"Gary_Fleder\" , \"Jon_Turteltaub\" , \"Robert_Lorenz\" , \"Josh_Gordon\" , \"Jennifer_Flackett\" , \"Phil_Joanou\" , \"Donald_Petrie\" , \"Martin_Campbell\" , \"Stephan_Elliott\" , \"Allen_Coulter\" , \"Ringo_Lam\" , \"Anthony_Minghella\" , \"Pang_Brothers\" , \"Danny_Boyle\" , \"Shana_Feste\" , \"Ivan_Reitman\" , \"Matt_Williams\" , \"Bart_Freundlich\" , \"Guy_Ferland\" , \"Peter_Cornwell\" , \"Josh_Schwartz\" , \"Robert_Mandel\" , \"Jason_Winer\" , \"Christopher_Guest\" , \"Shekhar_Kapur\" , \"Luis_Mandoki\" , \"Martin_McDonagh\" , \"Jimmy_Hayward\" , \"Mark_Piznarski\" , \"Rob_Reiner\" , \"Heitor_Dhalia\" , \"Mark_Dindal\" , \"Jay_Roach\" , \"Kevin_Hooks\" , \"Douglas_McGrath\" , \"Dennis_Dugan\" , \"Rick_Famuyiwa\" , \"Chris_Robinson\" , \"Roman_Polanksi\" , \"Alfonso_Cuarón\" , \"Peter_Webber\" , \"Rob_Marshall\" , \"Robert_Rodriguez\" , \"David_Bowers\" , \"David_O._Russell\" , \"Rich_Moore\" , \"Rob_Hardy\" , \"Quentin_Tarantino\" , \"John_Madden\" , \"Carl_Rinsch\" , \"Paul_Verhoeven\" , \"George_Armitage\" , \"Steve_McQueen\" , \"Dennis_Iliadis\" , \"Iain_Softley\" , \"Nat_Faxon,_Jim_Rash\" , \"Clint_Eastwood\" , \"David_Fincher\" , \"Jason_Reitman\" , \"Kasi_Lemmons\" , \"Joel_and_Ethan_Coen\" , \"Colin_and_Greg_Strause\" , \"Ron_Howard\" , \"Ken_Alterman\" , \"Risa_Bramon_Garcia\" , \"John_Moore\" , \"Carroll_Ballard\" , \"Doug_McHenry\" , \"Guillermo_Del_Toro\" , \"Tom_Dey\" , \"Fraser_Clarke_Heston\" , \"Craig_Mazin\" , \"Neil_Abramson\" , \"Scott_Hicks\" , \"Raja_Gosnell\" , \"Amy_Heckerling\" , \"Les_Mayfield\" , \"Sephen_Frears\" , \"Rob_Letterman\" , \"Nora_Ephron\" , \"Sacha_Gervasi\" , \"Gina_Prince-Bythewood\" , \"Michael_Moore\" , \"Paul_Feig\" , \"Asghar_Farhadi\" , \"James_Wan\" , \"Tim_Hill\" , \"Nathan_Greno\" , \"Vicky_Jenson\" , \"Todd_Haynes\" , \"Jonathan_Frakes\" , \"Richard_Linklater\" , \"Baltasar_Kormakur\" , \"Michael_Katleman\" , \"Jonathan_Levine\" , \"George_Clooney\" , \"Paul_Weiland\" , \"Tom_Shadyac\" , \"Christian_Alvart\" , \"Sam_Raimi\" , \"Hayao_Miyazaki\" , \"David_Twohy\" , \"Randall_Miller\" , \"Jan_Sv?rák\" , \"Frank_Marshall\" , \"Paul_Haggis\" , \"Drew_Goddard\" , \"Jason_Friedberg\" , \"Hideo_Nakata\" , \"Tony_Kaye\" , \"David_S._Goyer\" , \"Jonathan_Dayton\" , \"Joseph_Ruben\" , \"Matthew_OCallaghan\" , \"Sylvain_Chomet\" , \"Ronald_F._Maxwell\" , \"Ronny_Yu\" , \"Mark_Herman\" , \"Takashi_Shimizu\" , \"Joss_Whedon\" , \"Curtis_Hanson\" , \"Peter_Weir\" , \"John_Lasseter\" , \"Keenan_Ivory_Wayans\" , \"James_Cameron\" , \"Greg_McLean\" , \"Frank_Darabont\" , \"Tate_Taylor\" , \"Tom_DeCerchio\" , \"Kevin_Tancharoen\" , \"Jonathan_Mostow\" , \"Gus_Van_Sant\" , \"Chris_Kentis\" , \"Martin_Weisz\" , \"Gary_Ross\" , \"Julie_Anne_Robinson\" , \"Paul_Thomas_Anderson\" , \"Todd_Phillips\" , \"Andrew_Davis\" , \"Maurice_Joyce\" , \"Barry_Sonnenfeld\" , \"Robert_Altman\" , \"Tom_Hanks\" , \"Peter_Hastings\" , \"Richard_Martin\" , \"James_L._Brooks\" , \"John_Hamburg\" , \"Chris_Buck\" , \"Terrence_Malick\" , \"Harald_Zwart\" , \"Stephen_Norrington\" , \"Albert_Brooks\" , \"Francis_Veber\" , \"Kevin_Macdonald\" , \"Joe_Wright\" , \"Pierre_Morel\" , \"Scott_Stewart\" , \"JJ_Abrams\" , \"Darren_Aronofsky\" , \"Burr_Steers\" , \"Debra_Granik\" , \"Spike_Jonze\" , \"Sean_Anders\" , \"Tom_Hooper\" , \"Jessey_Terrero\" , \"Dave_Meyers\" , \"Brad_Bird\" , \"Bob_Spiers\" , \"Steven_Quale\" , \"Todd_Field\" , \"Christopher_Nolan\" , \"Craig_Bolotin\" , \"Andy_&_Larry_Wachowski\" , \"Richard_Donner\" , \"Lone_Scherfig\" , \"John_Lee_Hancock\" , \"Betty_Thomas\" , \"James_McTeigue\" , \"Rob_Cohen\" , \"Paul_Greengrass\" , \"Robert_Luketic\" , \"Zack_Snyder\" , \"Tommy_OHaver\" , \"John_McTiernan\" , \"David_R._Ellis\" , \"Martin_Scorsese\" , \"George_Tillman\" , \"Peter_Segal\" , \"Angela_Robinson\" , \"Mark_Andrews,_Brenda_Chapman\" , \"Daniel_Alfredson\" , \"Luke_Greenfield\" , \"Edgar_Wright\" , \"David_Talbert\" , \"Ricky_Gervais\" , \"Evan_Goldberg,_Seth_Rogen\" , \"Roger_Kumble\" , \"Stanley_Tong\" , \"Tony_Gilroy\" , \"Harold_Ramis\" , \"Gore_Verbinski\" , \"Kirk_Jones\" , \"Mark_Waters\" , \"Ryan_Murphy\" , \"Wes_Anderson\" , \"Wes_Craven\" , \"Ang_Lee\" , \"Marcus_Nispel\" , \"J.C._Chandor\" , \"Michael_Bay\" , \"Sam_Mendes\" , \"Pete_Docter\" , \"Deborah_Kaplan\" , \"Mike_Judge\" , \"John_Carpenter\" , \"Troy_Miller\" , \"Paul_W._S._Anderson\" , \"Andrew_Douglas\" , \"Alex_Kurtzman\" , \"Roger_Donaldson\" , \"Sean_McNamara\" , \"Tom_Tykwer\" , \"Miguel_Arteta\" , \"Michael_Caton-Jones\" , \"Tim_Burton\" , \"Howard_Deutch\" , \"Brian_Percival\" , \"Silvio_Soldini\" , \"Stephen_J._Anderson\" , \"Neveldine-Taylor\" , \"Mira_Nair\" , \"Joseph_Gordon-Levitt\" , \"Adam_Shankman\" , \"Matthijs_van_Heijningen_Jr.\" , \"Kathryn_Bigelow\" , \"Paul_Weitz\" , \"Catherine_Hardwicke\" , \"Chris_Gorak\" , \"Stephan_Frears\" , \"Julian_Jarrold\" , \"David_Yates\" , \"Sam_Fell\" , \"Roberto_Benigni\" , \"Baz_Luhrmann\" , \"The_Guard_Brothers\" , \"Gregory_Hoblit\" , \"Chris_Sanders\" , \"Christian_Duguay\" , \"Jake_Kasdan\" , \"Neill_Blomkamp\" , \"Marc_Lawrence\" , \"Todd_Graff\" , \"Gavin_Hood\" , \"Scott_Silver\" , \"Kevin_Costner\" , \"Ericson_Core\" , \"Jean-Marc_Vallee\" , \"Taylor_Hackford\" , \"Lee_Unkrich\" , \"RZA\" , \"Lasse_Hallström\" , \"Courtney_Solomon\" , \"J.J._Abrams\" , \"Tony_Scott\" , \"Jeff_Wadlow\" , \"Ryan_Coogler\" , \"Randall_Wallace\" , \"Stephen_Daldry\" , \"Ben_Affleck\" , \"Jon_Favreau\" , \"Kurt_Wimmer\" , \"John_Nicolella\" , \"Mike_Leigh\" , \"Andy_Tennant\" , \"Kimberly_Peirce\" , \"Woody_Allen\" , \"Nancy_Meyers\" , \"Ridley_Scott\" , \"Gavin_OConnor\" , \"Bennett_Miller\" , \"Ron_Clements\" , \"Ed_Harris\" , \"David_Slade\" , \"Mark_Brown\" ] ) )";
		// failing due to pandas DF not being able to run ,drop_duplicates() on a DataFramGroupBy object
		// line 260 of PandasInterpreter.java
		ApiSemossTestUtils.processPixel(cbvPixel);
	}
	
	private PandasFrame runRunDataQualityPixel(String rule, String column, String options, String inputTable) {
		PandasFrame newFrame = null;
		String pixel = ApiSemossTestUtils.buildPixelCall(RunDataQualityReactor.class,
				"rule", rule, 
				"column", column,
				"options", options,
				"inputTable", inputTable);
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		newFrame = (PandasFrame) noun.getValue();
		return newFrame;
	}
}
