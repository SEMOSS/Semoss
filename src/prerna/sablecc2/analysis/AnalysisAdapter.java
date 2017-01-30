/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.analysis;

import java.util.*;
import prerna.sablecc2.node.*;

public class AnalysisAdapter implements Analysis
{
    private Hashtable<Node,Object> in;
    private Hashtable<Node,Object> out;

    @Override
    public Object getIn(Node node)
    {
        if(this.in == null)
        {
            return null;
        }

        return this.in.get(node);
    }

    @Override
    public void setIn(Node node, Object o)
    {
        if(this.in == null)
        {
            this.in = new Hashtable<Node,Object>(1);
        }

        if(o != null)
        {
            this.in.put(node, o);
        }
        else
        {
            this.in.remove(node);
        }
    }

    @Override
    public Object getOut(Node node)
    {
        if(this.out == null)
        {
            return null;
        }

        return this.out.get(node);
    }

    @Override
    public void setOut(Node node, Object o)
    {
        if(this.out == null)
        {
            this.out = new Hashtable<Node,Object>(1);
        }

        if(o != null)
        {
            this.out.put(node, o);
        }
        else
        {
            this.out.remove(node);
        }
    }

    @Override
    public void caseStart(Start node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAConfiguration(AConfiguration node)
    {
        defaultCase(node);
    }

    @Override
    public void caseARefColDef(ARefColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseALiteralColDef(ALiteralColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAExprColDef(AExprColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseADotcolColDef(ADotcolColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFrameopColDef(AFrameopColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFilterColDef(AFilterColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAPropColDef(APropColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseARelationColDef(ARelationColDef node)
    {
        defaultCase(node);
    }

    @Override
    public void caseARcol(ARcol node)
    {
        defaultCase(node);
    }

    @Override
    public void caseADotcol(ADotcol node)
    {
        defaultCase(node);
    }

    @Override
    public void caseALiteral(ALiteral node)
    {
        defaultCase(node);
    }

    @Override
    public void caseADecimal(ADecimal node)
    {
        defaultCase(node);
    }

    @Override
    public void caseANumNumberOrLiteral(ANumNumberOrLiteral node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAStrNumberOrLiteral(AStrNumberOrLiteral node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAProp(AProp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseATermExpr(ATermExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAPlusExpr(APlusExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAMinusExpr(AMinusExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAMultExpr(AMultExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseADivExpr(ADivExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAModExpr(AModExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAEExprExpr(AEExprExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAExtendedExpr(AExtendedExpr node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFormula(AFormula node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOperationFormula(AOperationFormula node)
    {
        defaultCase(node);
    }

    @Override
    public void caseANumberTerm(ANumberTerm node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFormulaTerm(AFormulaTerm node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOpformulaTerm(AOpformulaTerm node)
    {
        defaultCase(node);
    }

    @Override
    public void caseABooleanTerm(ABooleanTerm node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAColTerm(AColTerm node)
    {
        defaultCase(node);
    }

    @Override
    public void caseACsvTerm(ACsvTerm node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOthercol(AOthercol node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAComparecol(AComparecol node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAGenRow(AGenRow node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAPlainRow(APlainRow node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAAnotherGenRow(AAnotherGenRow node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAGenTable(AGenTable node)
    {
        defaultCase(node);
    }

    @Override
    public void caseASelectors(ASelectors node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAProjectors(AProjectors node)
    {
        defaultCase(node);
    }

    @Override
    public void caseALabels(ALabels node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAProps(AProps node)
    {
        defaultCase(node);
    }

    @Override
    public void caseATooltips(ATooltips node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAJoins(AJoins node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAGeneric(AGeneric node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAAssignment(AAssignment node)
    {
        defaultCase(node);
    }

    @Override
    public void caseASelectNoun(ASelectNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAProjectNoun(AProjectNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseALabelsNoun(ALabelsNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseATooltipsNoun(ATooltipsNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOthersNoun(AOthersNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAPropsNoun(APropsNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseACodeNoun(ACodeNoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOthernoun(AOthernoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFrameopScript(AFrameopScript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAJavaOpScript(AJavaOpScript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAROpScript(AROpScript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOpScript(AOpScript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAMakeScript(AMakeScript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAAssignScript(AAssignScript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAOtherscript(AOtherscript node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAScriptchain(AScriptchain node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFrameop(AFrameop node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAAsop(AAsop node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAPimport(APimport node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAPnoun(APnoun node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFilter(AFilter node)
    {
        defaultCase(node);
    }

    @Override
    public void caseARelationship(ARelationship node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAJavaOp(AJavaOp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAROp(AROp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAApiBlock(AApiBlock node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFrameType(AFrameType node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAMakeData(AMakeData node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAMakeData2(AMakeData2 node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAMoveFrame(AMoveFrame node)
    {
        defaultCase(node);
    }

    @Override
    public void caseADataMakeMakeOp(ADataMakeMakeOp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAData2MakeMakeOp(AData2MakeMakeOp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseAFrameMoveMakeOp(AFrameMoveMakeOp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTNumber(TNumber node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTBoolean(TBoolean node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTSort(TSort node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTId(TId node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTDot(TDot node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTCodeAlpha(TCodeAlpha node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTSemicolon(TSemicolon node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTColon(TColon node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTPlus(TPlus node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTMinus(TMinus node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTMod(TMod node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTQuote(TQuote node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTWord(TWord node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTMult(TMult node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTComma(TComma node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTDiv(TDiv node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTComparator(TComparator node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTVizType(TVizType node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTLogOperator(TLogOperator node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTEqual(TEqual node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTSelectorid(TSelectorid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTGroupid(TGroupid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTOptionid(TOptionid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTProjectid(TProjectid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTPropid(TPropid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTLabelid(TLabelid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTJoinid(TJoinid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTTooltipid(TTooltipid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTFrameid(TFrameid node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTLPar(TLPar node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTRPar(TRPar node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTLBrac(TLBrac node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTRBrac(TRBrac node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTFrameprefix(TFrameprefix node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTBlank(TBlank node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTColprefix(TColprefix node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTKey(TKey node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTOptionKey(TOptionKey node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTTolookup(TTolookup node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTApi(TApi node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTOutput(TOutput node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTImportblock(TImportblock node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTJava(TJava node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTR(TR node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTMk(TMk node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTMv(TMv node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTAsOp(TAsOp node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTMap(TMap node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTFtype(TFtype node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTFrameVerbs(TFrameVerbs node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTCustom(TCustom node)
    {
        defaultCase(node);
    }

    @Override
    public void caseTRelType(TRelType node)
    {
        defaultCase(node);
    }

    @Override
    public void caseEOF(EOF node)
    {
        defaultCase(node);
    }

    @Override
    public void caseInvalidToken(InvalidToken node)
    {
        defaultCase(node);
    }

    public void defaultCase(@SuppressWarnings("unused") Node node)
    {
        // do nothing
    }
}
