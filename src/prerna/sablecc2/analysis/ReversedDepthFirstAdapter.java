/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.analysis;

import java.util.*;
import prerna.sablecc2.node.*;

public class ReversedDepthFirstAdapter extends AnalysisAdapter
{
    public void inStart(Start node)
    {
        defaultIn(node);
    }

    public void outStart(Start node)
    {
        defaultOut(node);
    }

    public void defaultIn(@SuppressWarnings("unused") Node node)
    {
        // Do nothing
    }

    public void defaultOut(@SuppressWarnings("unused") Node node)
    {
        // Do nothing
    }

    @Override
    public void caseStart(Start node)
    {
        inStart(node);
        node.getEOF().apply(this);
        node.getPConfiguration().apply(this);
        outStart(node);
    }

    public void inAConfiguration(AConfiguration node)
    {
        defaultIn(node);
    }

    public void outAConfiguration(AConfiguration node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAConfiguration(AConfiguration node)
    {
        inAConfiguration(node);
        {
            List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
            Collections.reverse(copy);
            for(PRoutine e : copy)
            {
                e.apply(this);
            }
        }
        outAConfiguration(node);
    }

    public void inAOutputRoutine(AOutputRoutine node)
    {
        defaultIn(node);
    }

    public void outAOutputRoutine(AOutputRoutine node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOutputRoutine(AOutputRoutine node)
    {
        inAOutputRoutine(node);
        if(node.getSemicolon() != null)
        {
            node.getSemicolon().apply(this);
        }
        if(node.getScriptchain() != null)
        {
            node.getScriptchain().apply(this);
        }
        outAOutputRoutine(node);
    }

    public void inAAssignRoutine(AAssignRoutine node)
    {
        defaultIn(node);
    }

    public void outAAssignRoutine(AAssignRoutine node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAAssignRoutine(AAssignRoutine node)
    {
        inAAssignRoutine(node);
        if(node.getSemicolon() != null)
        {
            node.getSemicolon().apply(this);
        }
        if(node.getAssignment() != null)
        {
            node.getAssignment().apply(this);
        }
        outAAssignRoutine(node);
    }

    public void inAScriptchain(AScriptchain node)
    {
        defaultIn(node);
    }

    public void outAScriptchain(AScriptchain node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAScriptchain(AScriptchain node)
    {
        inAScriptchain(node);
        {
            List<POtherscript> copy = new ArrayList<POtherscript>(node.getOtherscript());
            Collections.reverse(copy);
            for(POtherscript e : copy)
            {
                e.apply(this);
            }
        }
        if(node.getScript() != null)
        {
            node.getScript().apply(this);
        }
        outAScriptchain(node);
    }

    public void inAOtherscript(AOtherscript node)
    {
        defaultIn(node);
    }

    public void outAOtherscript(AOtherscript node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOtherscript(AOtherscript node)
    {
        inAOtherscript(node);
        if(node.getScript() != null)
        {
            node.getScript().apply(this);
        }
        if(node.getCustom() != null)
        {
            node.getCustom().apply(this);
        }
        outAOtherscript(node);
    }

    public void inAAssignment(AAssignment node)
    {
        defaultIn(node);
    }

    public void outAAssignment(AAssignment node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAAssignment(AAssignment node)
    {
        inAAssignment(node);
        if(node.getScriptchain() != null)
        {
            node.getScriptchain().apply(this);
        }
        if(node.getEqual() != null)
        {
            node.getEqual().apply(this);
        }
        if(node.getWordOrId() != null)
        {
            node.getWordOrId().apply(this);
        }
        outAAssignment(node);
    }

    public void inAExpressionScript(AExpressionScript node)
    {
        defaultIn(node);
    }

    public void outAExpressionScript(AExpressionScript node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAExpressionScript(AExpressionScript node)
    {
        inAExpressionScript(node);
        if(node.getExpr() != null)
        {
            node.getExpr().apply(this);
        }
        outAExpressionScript(node);
    }

    public void inAEmbeddedAssignmentScript(AEmbeddedAssignmentScript node)
    {
        defaultIn(node);
    }

    public void outAEmbeddedAssignmentScript(AEmbeddedAssignmentScript node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAEmbeddedAssignmentScript(AEmbeddedAssignmentScript node)
    {
        inAEmbeddedAssignmentScript(node);
        if(node.getEmbeddedAssignment() != null)
        {
            node.getEmbeddedAssignment().apply(this);
        }
        outAEmbeddedAssignmentScript(node);
    }

    public void inAEmbeddedAssignment(AEmbeddedAssignment node)
    {
        defaultIn(node);
    }

    public void outAEmbeddedAssignment(AEmbeddedAssignment node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAEmbeddedAssignment(AEmbeddedAssignment node)
    {
        inAEmbeddedAssignment(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        if(node.getScript() != null)
        {
            node.getScript().apply(this);
        }
        if(node.getEqual() != null)
        {
            node.getEqual().apply(this);
        }
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        outAEmbeddedAssignment(node);
    }

    public void inATermExpr(ATermExpr node)
    {
        defaultIn(node);
    }

    public void outATermExpr(ATermExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseATermExpr(ATermExpr node)
    {
        inATermExpr(node);
        if(node.getTerm() != null)
        {
            node.getTerm().apply(this);
        }
        outATermExpr(node);
    }

    public void inAPlusExpr(APlusExpr node)
    {
        defaultIn(node);
    }

    public void outAPlusExpr(APlusExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAPlusExpr(APlusExpr node)
    {
        inAPlusExpr(node);
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        if(node.getPlus() != null)
        {
            node.getPlus().apply(this);
        }
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        outAPlusExpr(node);
    }

    public void inAMinusExpr(AMinusExpr node)
    {
        defaultIn(node);
    }

    public void outAMinusExpr(AMinusExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAMinusExpr(AMinusExpr node)
    {
        inAMinusExpr(node);
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        if(node.getMinus() != null)
        {
            node.getMinus().apply(this);
        }
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        outAMinusExpr(node);
    }

    public void inAMultExpr(AMultExpr node)
    {
        defaultIn(node);
    }

    public void outAMultExpr(AMultExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAMultExpr(AMultExpr node)
    {
        inAMultExpr(node);
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        if(node.getMult() != null)
        {
            node.getMult().apply(this);
        }
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        outAMultExpr(node);
    }

    public void inADivExpr(ADivExpr node)
    {
        defaultIn(node);
    }

    public void outADivExpr(ADivExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseADivExpr(ADivExpr node)
    {
        inADivExpr(node);
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        if(node.getDiv() != null)
        {
            node.getDiv().apply(this);
        }
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        outADivExpr(node);
    }

    public void inAModExpr(AModExpr node)
    {
        defaultIn(node);
    }

    public void outAModExpr(AModExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAModExpr(AModExpr node)
    {
        inAModExpr(node);
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        if(node.getMod() != null)
        {
            node.getMod().apply(this);
        }
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        outAModExpr(node);
    }

    public void inAPowExpr(APowExpr node)
    {
        defaultIn(node);
    }

    public void outAPowExpr(APowExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAPowExpr(APowExpr node)
    {
        inAPowExpr(node);
        if(node.getRight() != null)
        {
            node.getRight().apply(this);
        }
        if(node.getPow() != null)
        {
            node.getPow().apply(this);
        }
        if(node.getLeft() != null)
        {
            node.getLeft().apply(this);
        }
        outAPowExpr(node);
    }

    public void inAScalarTerm(AScalarTerm node)
    {
        defaultIn(node);
    }

    public void outAScalarTerm(AScalarTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAScalarTerm(AScalarTerm node)
    {
        inAScalarTerm(node);
        if(node.getScalar() != null)
        {
            node.getScalar().apply(this);
        }
        outAScalarTerm(node);
    }

    public void inAFormulaTerm(AFormulaTerm node)
    {
        defaultIn(node);
    }

    public void outAFormulaTerm(AFormulaTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAFormulaTerm(AFormulaTerm node)
    {
        inAFormulaTerm(node);
        if(node.getFormula() != null)
        {
            node.getFormula().apply(this);
        }
        outAFormulaTerm(node);
    }

    public void inAOpformulaTerm(AOpformulaTerm node)
    {
        defaultIn(node);
    }

    public void outAOpformulaTerm(AOpformulaTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOpformulaTerm(AOpformulaTerm node)
    {
        inAOpformulaTerm(node);
        if(node.getOperationFormula() != null)
        {
            node.getOperationFormula().apply(this);
        }
        outAOpformulaTerm(node);
    }

    public void inAFrameopTerm(AFrameopTerm node)
    {
        defaultIn(node);
    }

    public void outAFrameopTerm(AFrameopTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAFrameopTerm(AFrameopTerm node)
    {
        inAFrameopTerm(node);
        if(node.getFrameop() != null)
        {
            node.getFrameop().apply(this);
        }
        outAFrameopTerm(node);
    }

    public void inAJavaOpTerm(AJavaOpTerm node)
    {
        defaultIn(node);
    }

    public void outAJavaOpTerm(AJavaOpTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAJavaOpTerm(AJavaOpTerm node)
    {
        inAJavaOpTerm(node);
        if(node.getJavaOp() != null)
        {
            node.getJavaOp().apply(this);
        }
        outAJavaOpTerm(node);
    }

    public void inAROpTerm(AROpTerm node)
    {
        defaultIn(node);
    }

    public void outAROpTerm(AROpTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAROpTerm(AROpTerm node)
    {
        inAROpTerm(node);
        if(node.getROp() != null)
        {
            node.getROp().apply(this);
        }
        outAROpTerm(node);
    }

    public void inAListTerm(AListTerm node)
    {
        defaultIn(node);
    }

    public void outAListTerm(AListTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAListTerm(AListTerm node)
    {
        inAListTerm(node);
        if(node.getList() != null)
        {
            node.getList().apply(this);
        }
        outAListTerm(node);
    }

    public void inACsvTerm(ACsvTerm node)
    {
        defaultIn(node);
    }

    public void outACsvTerm(ACsvTerm node)
    {
        defaultOut(node);
    }

    @Override
    public void caseACsvTerm(ACsvTerm node)
    {
        inACsvTerm(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        outACsvTerm(node);
    }

    public void inAFormula(AFormula node)
    {
        defaultIn(node);
    }

    public void outAFormula(AFormula node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAFormula(AFormula node)
    {
        inAFormula(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        if(node.getExpr() != null)
        {
            node.getExpr().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        outAFormula(node);
    }

    public void inAList(AList node)
    {
        defaultIn(node);
    }

    public void outAList(AList node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAList(AList node)
    {
        inAList(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        {
            List<POtherExpr> copy = new ArrayList<POtherExpr>(node.getOtherExpr());
            Collections.reverse(copy);
            for(POtherExpr e : copy)
            {
                e.apply(this);
            }
        }
        if(node.getExpr() != null)
        {
            node.getExpr().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        outAList(node);
    }

    public void inAOtherExpr(AOtherExpr node)
    {
        defaultIn(node);
    }

    public void outAOtherExpr(AOtherExpr node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOtherExpr(AOtherExpr node)
    {
        inAOtherExpr(node);
        if(node.getExpr() != null)
        {
            node.getExpr().apply(this);
        }
        if(node.getComma() != null)
        {
            node.getComma().apply(this);
        }
        outAOtherExpr(node);
    }

    public void inAExprColDef(AExprColDef node)
    {
        defaultIn(node);
    }

    public void outAExprColDef(AExprColDef node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAExprColDef(AExprColDef node)
    {
        inAExprColDef(node);
        if(node.getExpr() != null)
        {
            node.getExpr().apply(this);
        }
        outAExprColDef(node);
    }

    public void inARefColDef(ARefColDef node)
    {
        defaultIn(node);
    }

    public void outARefColDef(ARefColDef node)
    {
        defaultOut(node);
    }

    @Override
    public void caseARefColDef(ARefColDef node)
    {
        inARefColDef(node);
        if(node.getRcol() != null)
        {
            node.getRcol().apply(this);
        }
        outARefColDef(node);
    }

    public void inADotcolColDef(ADotcolColDef node)
    {
        defaultIn(node);
    }

    public void outADotcolColDef(ADotcolColDef node)
    {
        defaultOut(node);
    }

    @Override
    public void caseADotcolColDef(ADotcolColDef node)
    {
        inADotcolColDef(node);
        if(node.getDotcol() != null)
        {
            node.getDotcol().apply(this);
        }
        outADotcolColDef(node);
    }

    public void inAFilterColDef(AFilterColDef node)
    {
        defaultIn(node);
    }

    public void outAFilterColDef(AFilterColDef node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAFilterColDef(AFilterColDef node)
    {
        inAFilterColDef(node);
        if(node.getFilter() != null)
        {
            node.getFilter().apply(this);
        }
        outAFilterColDef(node);
    }

    public void inAPropColDef(APropColDef node)
    {
        defaultIn(node);
    }

    public void outAPropColDef(APropColDef node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAPropColDef(APropColDef node)
    {
        inAPropColDef(node);
        if(node.getProp() != null)
        {
            node.getProp().apply(this);
        }
        outAPropColDef(node);
    }

    public void inARelationColDef(ARelationColDef node)
    {
        defaultIn(node);
    }

    public void outARelationColDef(ARelationColDef node)
    {
        defaultOut(node);
    }

    @Override
    public void caseARelationColDef(ARelationColDef node)
    {
        inARelationColDef(node);
        if(node.getRelationship() != null)
        {
            node.getRelationship().apply(this);
        }
        outARelationColDef(node);
    }

    public void inAOthercol(AOthercol node)
    {
        defaultIn(node);
    }

    public void outAOthercol(AOthercol node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOthercol(AOthercol node)
    {
        inAOthercol(node);
        if(node.getColDef() != null)
        {
            node.getColDef().apply(this);
        }
        if(node.getComma() != null)
        {
            node.getComma().apply(this);
        }
        outAOthercol(node);
    }

    public void inAGenRow(AGenRow node)
    {
        defaultIn(node);
    }

    public void outAGenRow(AGenRow node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAGenRow(AGenRow node)
    {
        inAGenRow(node);
        if(node.getRBrac() != null)
        {
            node.getRBrac().apply(this);
        }
        {
            List<POthercol> copy = new ArrayList<POthercol>(node.getOthercol());
            Collections.reverse(copy);
            for(POthercol e : copy)
            {
                e.apply(this);
            }
        }
        if(node.getColDef() != null)
        {
            node.getColDef().apply(this);
        }
        if(node.getLBrac() != null)
        {
            node.getLBrac().apply(this);
        }
        outAGenRow(node);
    }

    public void inAPlainRow(APlainRow node)
    {
        defaultIn(node);
    }

    public void outAPlainRow(APlainRow node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAPlainRow(APlainRow node)
    {
        inAPlainRow(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        {
            List<POthercol> copy = new ArrayList<POthercol>(node.getOthercol());
            Collections.reverse(copy);
            for(POthercol e : copy)
            {
                e.apply(this);
            }
        }
        if(node.getColDef() != null)
        {
            node.getColDef().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        outAPlainRow(node);
    }

    public void inAOperationFormula(AOperationFormula node)
    {
        defaultIn(node);
    }

    public void outAOperationFormula(AOperationFormula node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOperationFormula(AOperationFormula node)
    {
        inAOperationFormula(node);
        if(node.getAsop() != null)
        {
            node.getAsop().apply(this);
        }
        if(node.getPlainRow() != null)
        {
            node.getPlainRow().apply(this);
        }
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        outAOperationFormula(node);
    }

    public void inARcol(ARcol node)
    {
        defaultIn(node);
    }

    public void outARcol(ARcol node)
    {
        defaultOut(node);
    }

    @Override
    public void caseARcol(ARcol node)
    {
        inARcol(node);
        if(node.getNumber() != null)
        {
            node.getNumber().apply(this);
        }
        if(node.getFrameprefix() != null)
        {
            node.getFrameprefix().apply(this);
        }
        outARcol(node);
    }

    public void inADotcol(ADotcol node)
    {
        defaultIn(node);
    }

    public void outADotcol(ADotcol node)
    {
        defaultOut(node);
    }

    @Override
    public void caseADotcol(ADotcol node)
    {
        inADotcol(node);
        if(node.getColumnName() != null)
        {
            node.getColumnName().apply(this);
        }
        if(node.getDot() != null)
        {
            node.getDot().apply(this);
        }
        if(node.getFrameid() != null)
        {
            node.getFrameid().apply(this);
        }
        outADotcol(node);
    }

    public void inAMinusPosOrNeg(AMinusPosOrNeg node)
    {
        defaultIn(node);
    }

    public void outAMinusPosOrNeg(AMinusPosOrNeg node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAMinusPosOrNeg(AMinusPosOrNeg node)
    {
        inAMinusPosOrNeg(node);
        if(node.getMinus() != null)
        {
            node.getMinus().apply(this);
        }
        outAMinusPosOrNeg(node);
    }

    public void inAPlusPosOrNeg(APlusPosOrNeg node)
    {
        defaultIn(node);
    }

    public void outAPlusPosOrNeg(APlusPosOrNeg node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAPlusPosOrNeg(APlusPosOrNeg node)
    {
        inAPlusPosOrNeg(node);
        if(node.getPlus() != null)
        {
            node.getPlus().apply(this);
        }
        outAPlusPosOrNeg(node);
    }

    public void inADecimal(ADecimal node)
    {
        defaultIn(node);
    }

    public void outADecimal(ADecimal node)
    {
        defaultOut(node);
    }

    @Override
    public void caseADecimal(ADecimal node)
    {
        inADecimal(node);
        if(node.getFraction() != null)
        {
            node.getFraction().apply(this);
        }
        if(node.getDot() != null)
        {
            node.getDot().apply(this);
        }
        if(node.getWhole() != null)
        {
            node.getWhole().apply(this);
        }
        if(node.getPosOrNeg() != null)
        {
            node.getPosOrNeg().apply(this);
        }
        outADecimal(node);
    }

    public void inAWordWordOrId(AWordWordOrId node)
    {
        defaultIn(node);
    }

    public void outAWordWordOrId(AWordWordOrId node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAWordWordOrId(AWordWordOrId node)
    {
        inAWordWordOrId(node);
        if(node.getWord() != null)
        {
            node.getWord().apply(this);
        }
        outAWordWordOrId(node);
    }

    public void inAIdWordOrId(AIdWordOrId node)
    {
        defaultIn(node);
    }

    public void outAIdWordOrId(AIdWordOrId node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAIdWordOrId(AIdWordOrId node)
    {
        inAIdWordOrId(node);
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        outAIdWordOrId(node);
    }

    public void inANumScalar(ANumScalar node)
    {
        defaultIn(node);
    }

    public void outANumScalar(ANumScalar node)
    {
        defaultOut(node);
    }

    @Override
    public void caseANumScalar(ANumScalar node)
    {
        inANumScalar(node);
        if(node.getDecimal() != null)
        {
            node.getDecimal().apply(this);
        }
        outANumScalar(node);
    }

    public void inAWordOrIdScalar(AWordOrIdScalar node)
    {
        defaultIn(node);
    }

    public void outAWordOrIdScalar(AWordOrIdScalar node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAWordOrIdScalar(AWordOrIdScalar node)
    {
        inAWordOrIdScalar(node);
        if(node.getWordOrId() != null)
        {
            node.getWordOrId().apply(this);
        }
        outAWordOrIdScalar(node);
    }

    public void inABooleanScalar(ABooleanScalar node)
    {
        defaultIn(node);
    }

    public void outABooleanScalar(ABooleanScalar node)
    {
        defaultOut(node);
    }

    @Override
    public void caseABooleanScalar(ABooleanScalar node)
    {
        inABooleanScalar(node);
        if(node.getBoolean() != null)
        {
            node.getBoolean().apply(this);
        }
        outABooleanScalar(node);
    }

    public void inAProp(AProp node)
    {
        defaultIn(node);
    }

    public void outAProp(AProp node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAProp(AProp node)
    {
        inAProp(node);
        if(node.getScalar() != null)
        {
            node.getScalar().apply(this);
        }
        if(node.getEqual() != null)
        {
            node.getEqual().apply(this);
        }
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        outAProp(node);
    }

    public void inASelectors(ASelectors node)
    {
        defaultIn(node);
    }

    public void outASelectors(ASelectors node)
    {
        defaultOut(node);
    }

    @Override
    public void caseASelectors(ASelectors node)
    {
        inASelectors(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getSelectorid() != null)
        {
            node.getSelectorid().apply(this);
        }
        outASelectors(node);
    }

    public void inAProjectors(AProjectors node)
    {
        defaultIn(node);
    }

    public void outAProjectors(AProjectors node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAProjectors(AProjectors node)
    {
        inAProjectors(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getProjectid() != null)
        {
            node.getProjectid().apply(this);
        }
        outAProjectors(node);
    }

    public void inALabels(ALabels node)
    {
        defaultIn(node);
    }

    public void outALabels(ALabels node)
    {
        defaultOut(node);
    }

    @Override
    public void caseALabels(ALabels node)
    {
        inALabels(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getLabelid() != null)
        {
            node.getLabelid().apply(this);
        }
        outALabels(node);
    }

    public void inAProps(AProps node)
    {
        defaultIn(node);
    }

    public void outAProps(AProps node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAProps(AProps node)
    {
        inAProps(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getPropid() != null)
        {
            node.getPropid().apply(this);
        }
        outAProps(node);
    }

    public void inATooltips(ATooltips node)
    {
        defaultIn(node);
    }

    public void outATooltips(ATooltips node)
    {
        defaultOut(node);
    }

    @Override
    public void caseATooltips(ATooltips node)
    {
        inATooltips(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getTooltipid() != null)
        {
            node.getTooltipid().apply(this);
        }
        outATooltips(node);
    }

    public void inAJoins(AJoins node)
    {
        defaultIn(node);
    }

    public void outAJoins(AJoins node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAJoins(AJoins node)
    {
        inAJoins(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getJoinid() != null)
        {
            node.getJoinid().apply(this);
        }
        outAJoins(node);
    }

    public void inAGeneric(AGeneric node)
    {
        defaultIn(node);
    }

    public void outAGeneric(AGeneric node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAGeneric(AGeneric node)
    {
        inAGeneric(node);
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getEqual() != null)
        {
            node.getEqual().apply(this);
        }
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        outAGeneric(node);
    }

    public void inASelectNoun(ASelectNoun node)
    {
        defaultIn(node);
    }

    public void outASelectNoun(ASelectNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseASelectNoun(ASelectNoun node)
    {
        inASelectNoun(node);
        if(node.getSelectors() != null)
        {
            node.getSelectors().apply(this);
        }
        outASelectNoun(node);
    }

    public void inAProjectNoun(AProjectNoun node)
    {
        defaultIn(node);
    }

    public void outAProjectNoun(AProjectNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAProjectNoun(AProjectNoun node)
    {
        inAProjectNoun(node);
        if(node.getProjectors() != null)
        {
            node.getProjectors().apply(this);
        }
        outAProjectNoun(node);
    }

    public void inALabelsNoun(ALabelsNoun node)
    {
        defaultIn(node);
    }

    public void outALabelsNoun(ALabelsNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseALabelsNoun(ALabelsNoun node)
    {
        inALabelsNoun(node);
        if(node.getLabels() != null)
        {
            node.getLabels().apply(this);
        }
        outALabelsNoun(node);
    }

    public void inATooltipsNoun(ATooltipsNoun node)
    {
        defaultIn(node);
    }

    public void outATooltipsNoun(ATooltipsNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseATooltipsNoun(ATooltipsNoun node)
    {
        inATooltipsNoun(node);
        if(node.getTooltips() != null)
        {
            node.getTooltips().apply(this);
        }
        outATooltipsNoun(node);
    }

    public void inAOthersNoun(AOthersNoun node)
    {
        defaultIn(node);
    }

    public void outAOthersNoun(AOthersNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOthersNoun(AOthersNoun node)
    {
        inAOthersNoun(node);
        if(node.getGeneric() != null)
        {
            node.getGeneric().apply(this);
        }
        outAOthersNoun(node);
    }

    public void inAPropsNoun(APropsNoun node)
    {
        defaultIn(node);
    }

    public void outAPropsNoun(APropsNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAPropsNoun(APropsNoun node)
    {
        inAPropsNoun(node);
        if(node.getProps() != null)
        {
            node.getProps().apply(this);
        }
        outAPropsNoun(node);
    }

    public void inACodeNoun(ACodeNoun node)
    {
        defaultIn(node);
    }

    public void outACodeNoun(ACodeNoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseACodeNoun(ACodeNoun node)
    {
        inACodeNoun(node);
        if(node.getCodeAlpha() != null)
        {
            node.getCodeAlpha().apply(this);
        }
        outACodeNoun(node);
    }

    public void inAOthernoun(AOthernoun node)
    {
        defaultIn(node);
    }

    public void outAOthernoun(AOthernoun node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAOthernoun(AOthernoun node)
    {
        inAOthernoun(node);
        if(node.getNoun() != null)
        {
            node.getNoun().apply(this);
        }
        if(node.getComma() != null)
        {
            node.getComma().apply(this);
        }
        outAOthernoun(node);
    }

    public void inAFrameop(AFrameop node)
    {
        defaultIn(node);
    }

    public void outAFrameop(AFrameop node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAFrameop(AFrameop node)
    {
        inAFrameop(node);
        if(node.getAsop() != null)
        {
            node.getAsop().apply(this);
        }
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        {
            List<POthernoun> copy = new ArrayList<POthernoun>(node.getOthernoun());
            Collections.reverse(copy);
            for(POthernoun e : copy)
            {
                e.apply(this);
            }
        }
        if(node.getNoun() != null)
        {
            node.getNoun().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        if(node.getId() != null)
        {
            node.getId().apply(this);
        }
        outAFrameop(node);
    }

    public void inAAsop(AAsop node)
    {
        defaultIn(node);
    }

    public void outAAsop(AAsop node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAAsop(AAsop node)
    {
        inAAsop(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        if(node.getGenRow() != null)
        {
            node.getGenRow().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        if(node.getAsOp() != null)
        {
            node.getAsOp().apply(this);
        }
        outAAsop(node);
    }

    public void inAFilter(AFilter node)
    {
        defaultIn(node);
    }

    public void outAFilter(AFilter node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAFilter(AFilter node)
    {
        inAFilter(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        if(node.getRcol() != null)
        {
            node.getRcol().apply(this);
        }
        if(node.getComparator() != null)
        {
            node.getComparator().apply(this);
        }
        if(node.getLcol() != null)
        {
            node.getLcol().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        outAFilter(node);
    }

    public void inARelationship(ARelationship node)
    {
        defaultIn(node);
    }

    public void outARelationship(ARelationship node)
    {
        defaultOut(node);
    }

    @Override
    public void caseARelationship(ARelationship node)
    {
        inARelationship(node);
        if(node.getRPar() != null)
        {
            node.getRPar().apply(this);
        }
        if(node.getRcol() != null)
        {
            node.getRcol().apply(this);
        }
        if(node.getRelType() != null)
        {
            node.getRelType().apply(this);
        }
        if(node.getLcol() != null)
        {
            node.getLcol().apply(this);
        }
        if(node.getLPar() != null)
        {
            node.getLPar().apply(this);
        }
        outARelationship(node);
    }

    public void inAJavaOp(AJavaOp node)
    {
        defaultIn(node);
    }

    public void outAJavaOp(AJavaOp node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAJavaOp(AJavaOp node)
    {
        inAJavaOp(node);
        if(node.getJava() != null)
        {
            node.getJava().apply(this);
        }
        outAJavaOp(node);
    }

    public void inAROp(AROp node)
    {
        defaultIn(node);
    }

    public void outAROp(AROp node)
    {
        defaultOut(node);
    }

    @Override
    public void caseAROp(AROp node)
    {
        inAROp(node);
        if(node.getR() != null)
        {
            node.getR().apply(this);
        }
        outAROp(node);
    }
}
