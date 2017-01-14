/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.analysis;

import prerna.sablecc2.node.*;

public interface Analysis extends Switch
{
    Object getIn(Node node);
    void setIn(Node node, Object o);
    Object getOut(Node node);
    void setOut(Node node, Object o);

    void caseStart(Start node);
    void caseAConfiguration(AConfiguration node);
    void caseARefColDef(ARefColDef node);
    void caseALiteralColDef(ALiteralColDef node);
    void caseAExprColDef(AExprColDef node);
    void caseADotcolColDef(ADotcolColDef node);
    void caseAFrameopColDef(AFrameopColDef node);
    void caseAFilterColDef(AFilterColDef node);
    void caseAPropColDef(APropColDef node);
    void caseARcol(ARcol node);
    void caseADotcol(ADotcol node);
    void caseALiteral(ALiteral node);
    void caseADecimal(ADecimal node);
    void caseANumNumberOrLiteral(ANumNumberOrLiteral node);
    void caseAStrNumberOrLiteral(AStrNumberOrLiteral node);
    void caseAProp(AProp node);
    void caseATermExpr(ATermExpr node);
    void caseAPlusExpr(APlusExpr node);
    void caseAMinusExpr(AMinusExpr node);
    void caseAMultExpr(AMultExpr node);
    void caseADivExpr(ADivExpr node);
    void caseAModExpr(AModExpr node);
    void caseAEExprExpr(AEExprExpr node);
    void caseAExtendedExpr(AExtendedExpr node);
    void caseAFormula(AFormula node);
    void caseAOperationFormula(AOperationFormula node);
    void caseANumberTerm(ANumberTerm node);
    void caseAFormulaTerm(AFormulaTerm node);
    void caseAOpformulaTerm(AOpformulaTerm node);
    void caseABooleanTerm(ABooleanTerm node);
    void caseAColTerm(AColTerm node);
    void caseACsvTerm(ACsvTerm node);
    void caseAOthercol(AOthercol node);
    void caseAComparecol(AComparecol node);
    void caseAGenRow(AGenRow node);
    void caseAPlainRow(APlainRow node);
    void caseAAnotherGenRow(AAnotherGenRow node);
    void caseAGenTable(AGenTable node);
    void caseASelectors(ASelectors node);
    void caseAProjectors(AProjectors node);
    void caseALabels(ALabels node);
    void caseAProps(AProps node);
    void caseATooltips(ATooltips node);
    void caseAJoins(AJoins node);
    void caseAGeneric(AGeneric node);
    void caseAAssignment(AAssignment node);
    void caseASelectNoun(ASelectNoun node);
    void caseAProjectNoun(AProjectNoun node);
    void caseALabelsNoun(ALabelsNoun node);
    void caseATooltipsNoun(ATooltipsNoun node);
    void caseAOthersNoun(AOthersNoun node);
    void caseAPropsNoun(APropsNoun node);
    void caseAOthernoun(AOthernoun node);
    void caseAFrameopScript(AFrameopScript node);
    void caseAJavaOpScript(AJavaOpScript node);
    void caseAROpScript(AROpScript node);
    void caseAOpScript(AOpScript node);
    void caseAMakeScript(AMakeScript node);
    void caseAAssignScript(AAssignScript node);
    void caseAMapScript(AMapScript node);
    void caseAOtherscript(AOtherscript node);
    void caseAScriptchain(AScriptchain node);
    void caseAFrameop(AFrameop node);
    void caseAAsop(AAsop node);
    void caseAMapOp(AMapOp node);
    void caseAPimport(APimport node);
    void caseAPnoun(APnoun node);
    void caseAFilter(AFilter node);
    void caseAJavaOp(AJavaOp node);
    void caseAROp(AROp node);
    void caseAApiBlock(AApiBlock node);
    void caseAFrameType(AFrameType node);
    void caseAMakeData(AMakeData node);
    void caseAMakeData2(AMakeData2 node);
    void caseAMoveFrame(AMoveFrame node);
    void caseADataMakeMakeOp(ADataMakeMakeOp node);
    void caseAData2MakeMakeOp(AData2MakeMakeOp node);
    void caseAFrameMoveMakeOp(AFrameMoveMakeOp node);

    void caseTNumber(TNumber node);
    void caseTBoolean(TBoolean node);
    void caseTSort(TSort node);
    void caseTId(TId node);
    void caseTDot(TDot node);
    void caseTSemicolon(TSemicolon node);
    void caseTColon(TColon node);
    void caseTPlus(TPlus node);
    void caseTMinus(TMinus node);
    void caseTMod(TMod node);
    void caseTQuote(TQuote node);
    void caseTWord(TWord node);
    void caseTMult(TMult node);
    void caseTComma(TComma node);
    void caseTDiv(TDiv node);
    void caseTComparator(TComparator node);
    void caseTVizType(TVizType node);
    void caseTLogOperator(TLogOperator node);
    void caseTEqual(TEqual node);
    void caseTSelectorid(TSelectorid node);
    void caseTGroupid(TGroupid node);
    void caseTOptionid(TOptionid node);
    void caseTProjectid(TProjectid node);
    void caseTPropid(TPropid node);
    void caseTLabelid(TLabelid node);
    void caseTJoinid(TJoinid node);
    void caseTTooltipid(TTooltipid node);
    void caseTFrameid(TFrameid node);
    void caseTLPar(TLPar node);
    void caseTRPar(TRPar node);
    void caseTLBrac(TLBrac node);
    void caseTRBrac(TRBrac node);
    void caseTFrameprefix(TFrameprefix node);
    void caseTBlank(TBlank node);
    void caseTColprefix(TColprefix node);
    void caseTKey(TKey node);
    void caseTOptionKey(TOptionKey node);
    void caseTTolookup(TTolookup node);
    void caseTApi(TApi node);
    void caseTOutput(TOutput node);
    void caseTImportblock(TImportblock node);
    void caseTCodeblock(TCodeblock node);
    void caseTJava(TJava node);
    void caseTR(TR node);
    void caseTMk(TMk node);
    void caseTMv(TMv node);
    void caseTAsOp(TAsOp node);
    void caseTMap(TMap node);
    void caseTFtype(TFtype node);
    void caseTFrameVerbs(TFrameVerbs node);
    void caseTCustom(TCustom node);
    void caseEOF(EOF node);
    void caseInvalidToken(InvalidToken node);
}
