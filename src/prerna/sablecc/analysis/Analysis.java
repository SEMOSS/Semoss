/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.analysis;

import prerna.sablecc.node.*;

public interface Analysis extends Switch
{
    Object getIn(Node node);
    void setIn(Node node, Object o);
    Object getOut(Node node);
    void setOut(Node node, Object o);

    void caseStart(Start node);
    void caseAConfiguration(AConfiguration node);
    void caseAColopScript(AColopScript node);
    void caseAVaropScript(AVaropScript node);
    void caseAExprScript(AExprScript node);
    void caseAHelpScript(AHelpScript node);
    void caseAPanelopScript(APanelopScript node);
    void caseAScript(AScript node);
    void caseAAddColumnColop(AAddColumnColop node);
    void caseARemcolColop(ARemcolColop node);
    void caseASetcolColop(ASetcolColop node);
    void caseAPivotcolColop(APivotcolColop node);
    void caseAFiltercolColop(AFiltercolColop node);
    void caseAFocuscolColop(AFocuscolColop node);
    void caseAUnfocusColop(AUnfocusColop node);
    void caseAImportColop(AImportColop node);
    void caseAAliasColop(AAliasColop node);
    void caseAImportDataColop(AImportDataColop node);
    void caseAUnfiltercolColop(AUnfiltercolColop node);
    void caseARemoveDataColop(ARemoveDataColop node);
    void caseADataFrameColop(ADataFrameColop node);
    void caseADashboardJoinColop(ADashboardJoinColop node);
    void caseAPanelVizPanelop(APanelVizPanelop node);
    void caseAPanelCommentPanelop(APanelCommentPanelop node);
    void caseAPanelCommentRemovePanelop(APanelCommentRemovePanelop node);
    void caseAPanelCommentEditPanelop(APanelCommentEditPanelop node);
    void caseAPanelLookAndFeelPanelop(APanelLookAndFeelPanelop node);
    void caseAPanelToolsPanelop(APanelToolsPanelop node);
    void caseAPanelConfigPanelop(APanelConfigPanelop node);
    void caseAPanelClonePanelop(APanelClonePanelop node);
    void caseAPanelClosePanelop(APanelClosePanelop node);
    void caseAOutputInsightPanelop(AOutputInsightPanelop node);
    void caseAPanelViz(APanelViz node);
    void caseAPanelComment(APanelComment node);
    void caseAPanelCommentEdit(APanelCommentEdit node);
    void caseAPanelCommentRemove(APanelCommentRemove node);
    void caseAPanelLookAndFeel(APanelLookAndFeel node);
    void caseAPanelTools(APanelTools node);
    void caseAPanelConfig(APanelConfig node);
    void caseAPanelClone(APanelClone node);
    void caseAPanelClose(APanelClose node);
    void caseADataFrame(ADataFrame node);
    void caseAAddColumn(AAddColumn node);
    void caseARemColumn(ARemColumn node);
    void caseASetColumn(ASetColumn node);
    void caseAPivotColumn(APivotColumn node);
    void caseAFilterColumn(AFilterColumn node);
    void caseAUnfilterColumn(AUnfilterColumn node);
    void caseAFocusColumn(AFocusColumn node);
    void caseAUnfocus(AUnfocus node);
    void caseAImportColumn(AImportColumn node);
    void caseAAliasColumn(AAliasColumn node);
    void caseAImportData(AImportData node);
    void caseAApiImportBlock(AApiImportBlock node);
    void caseACsvTableImportBlock(ACsvTableImportBlock node);
    void caseAPastedDataImportBlock(APastedDataImportBlock node);
    void caseAPastedDataBlock(APastedDataBlock node);
    void caseAPastedData(APastedData node);
    void caseARemoveData(ARemoveData node);
    void caseADecimal(ADecimal node);
    void caseAExprGroup(AExprGroup node);
    void caseAOutputInsight(AOutputInsight node);
    void caseAApiBlock(AApiBlock node);
    void caseASelector(ASelector node);
    void caseAColWhere(AColWhere node);
    void caseAColDefColDefOrCsvRow(AColDefColDefOrCsvRow node);
    void caseACsvColDefOrCsvRow(ACsvColDefOrCsvRow node);
    void caseAColWhereGroup(AColWhereGroup node);
    void caseAWhereClause(AWhereClause node);
    void caseAWhereStatement(AWhereStatement node);
    void caseARelationDef(ARelationDef node);
    void caseARelationGroup(ARelationGroup node);
    void caseARelationClause(ARelationClause node);
    void caseAIfBlock(AIfBlock node);
    void caseAColGroup(AColGroup node);
    void caseAKeyvalue(AKeyvalue node);
    void caseAKeyvalueGroup(AKeyvalueGroup node);
    void caseAMapObj(AMapObj node);
    void caseAGroupBy(AGroupBy node);
    void caseAColDef(AColDef node);
    void caseATableDef(ATableDef node);
    void caseAVarop(AVarop node);
    void caseACsvRow(ACsvRow node);
    void caseAEasyRow(AEasyRow node);
    void caseAEasyGroup(AEasyGroup node);
    void caseACsvTable(ACsvTable node);
    void caseAColCsv(AColCsv node);
    void caseANumWordOrNum(ANumWordOrNum node);
    void caseAAlphaWordOrNum(AAlphaWordOrNum node);
    void caseAExprWordOrNum(AExprWordOrNum node);
    void caseAWordOrNumWordOrNumOrNestedObj(AWordOrNumWordOrNumOrNestedObj node);
    void caseANestedMapWordOrNumOrNestedObj(ANestedMapWordOrNumOrNestedObj node);
    void caseANestedCsvWordOrNumOrNestedObj(ANestedCsvWordOrNumOrNestedObj node);
    void caseAFlexSelectorRow(AFlexSelectorRow node);
    void caseATermGroup(ATermGroup node);
    void caseAFormula(AFormula node);
    void caseACsvGroup(ACsvGroup node);
    void caseAExprRow(AExprRow node);
    void caseADashboardJoin(ADashboardJoin node);
    void caseAJOp(AJOp node);
    void caseAHelp(AHelp node);
    void caseAComparatorEqualOrCompare(AComparatorEqualOrCompare node);
    void caseAEqualEqualOrCompare(AEqualEqualOrCompare node);
    void caseAUserInput(AUserInput node);
    void caseAExprInputOrExpr(AExprInputOrExpr node);
    void caseAInputInputOrExpr(AInputInputOrExpr node);
    void caseATermExpr(ATermExpr node);
    void caseAPlusExpr(APlusExpr node);
    void caseAMinusExpr(AMinusExpr node);
    void caseAMultExpr(AMultExpr node);
    void caseAExpr(AExpr node);
    void caseADivExpr(ADivExpr node);
    void caseAModExpr(AModExpr node);
    void caseAEExprExpr(AEExprExpr node);
    void caseAMathFun(AMathFun node);
    void caseAMathParam(AMathParam node);
    void caseAExtendedExpr(AExtendedExpr node);
    void caseANumberTerm(ANumberTerm node);
    void caseAExprTerm(AExprTerm node);
    void caseAVarTerm(AVarTerm node);
    void caseAColTerm(AColTerm node);
    void caseAApiTerm(AApiTerm node);
    void caseATabTerm(ATabTerm node);
    void caseAWcsvTerm(AWcsvTerm node);
    void caseATerm(ATerm node);
    void caseAAlphaTerm(AAlphaTerm node);
    void caseAMathFunTerm(AMathFunTerm node);
    void caseACodeblockTerm(ACodeblockTerm node);

    void caseTNumber(TNumber node);
    void caseTId(TId node);
    void caseTDot(TDot node);
    void caseTSemicolon(TSemicolon node);
    void caseTColon(TColon node);
    void caseTPlus(TPlus node);
    void caseTMinus(TMinus node);
    void caseTMult(TMult node);
    void caseTComma(TComma node);
    void caseTDiv(TDiv node);
    void caseTCol(TCol node);
    void caseTComparator(TComparator node);
    void caseTEqual(TEqual node);
    void caseTColadd(TColadd node);
    void caseTApi(TApi node);
    void caseTMath(TMath node);
    void caseTColjoin(TColjoin node);
    void caseTColprefix(TColprefix node);
    void caseTTablePrefix(TTablePrefix node);
    void caseTValprefix(TValprefix node);
    void caseTColremove(TColremove node);
    void caseTColfilter(TColfilter node);
    void caseTColunfilter(TColunfilter node);
    void caseTColimport(TColimport node);
    void caseTColset(TColset node);
    void caseTColpivot(TColpivot node);
    void caseTColfocus(TColfocus node);
    void caseTColalias(TColalias node);
    void caseTCollink(TCollink node);
    void caseTShowHide(TShowHide node);
    void caseTMod(TMod node);
    void caseTLPar(TLPar node);
    void caseTRPar(TRPar node);
    void caseTLBracket(TLBracket node);
    void caseTRBracket(TRBracket node);
    void caseTLCurlBracket(TLCurlBracket node);
    void caseTRCurlBracket(TRCurlBracket node);
    void caseTGroup(TGroup node);
    void caseTBlank(TBlank node);
    void caseTSpace(TSpace node);
    void caseTNewline(TNewline node);
    void caseTJava(TJava node);
    void caseTPython(TPython node);
    void caseTProc(TProc node);
    void caseTThis(TThis node);
    void caseTNull(TNull node);
    void caseTImportType(TImportType node);
    void caseTRelType(TRelType node);
    void caseTDataimporttoken(TDataimporttoken node);
    void caseTDataremovetoken(TDataremovetoken node);
    void caseTLiteral(TLiteral node);
    void caseTHelpToken(THelpToken node);
    void caseTCodeblock(TCodeblock node);
    void caseTWord(TWord node);
    void caseTPanelviz(TPanelviz node);
    void caseTPanelclone(TPanelclone node);
    void caseTPanelclose(TPanelclose node);
    void caseTDataframe(TDataframe node);
    void caseTFileText(TFileText node);
    void caseTPanelcommentremove(TPanelcommentremove node);
    void caseTPanelcommentedit(TPanelcommentedit node);
    void caseTPanelcommentadd(TPanelcommentadd node);
    void caseTPanellookandfeel(TPanellookandfeel node);
    void caseTPaneltools(TPaneltools node);
    void caseTPanelconfig(TPanelconfig node);
    void caseTOutputToken(TOutputToken node);
    void caseTUserinput(TUserinput node);
    void caseTJoin(TJoin node);
    void caseEOF(EOF node);
    void caseInvalidToken(InvalidToken node);
}
