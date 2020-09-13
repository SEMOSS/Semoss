/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class AMathFun extends PMathFun
{
    private TProc _proc_;
    private TId _id_;
    private TLPar _lPar_;
    private PExprRow _expr_;
    private TComma _comma_;
    private PColCsv _group_;
    private POptionsMap _parameters_;
    private TRPar _rPar_;

    public AMathFun()
    {
        // Constructor
    }

    public AMathFun(
        @SuppressWarnings("hiding") TProc _proc_,
        @SuppressWarnings("hiding") TId _id_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") PExprRow _expr_,
        @SuppressWarnings("hiding") TComma _comma_,
        @SuppressWarnings("hiding") PColCsv _group_,
        @SuppressWarnings("hiding") POptionsMap _parameters_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setProc(_proc_);

        setId(_id_);

        setLPar(_lPar_);

        setExpr(_expr_);

        setComma(_comma_);

        setGroup(_group_);

        setParameters(_parameters_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new AMathFun(
            cloneNode(this._proc_),
            cloneNode(this._id_),
            cloneNode(this._lPar_),
            cloneNode(this._expr_),
            cloneNode(this._comma_),
            cloneNode(this._group_),
            cloneNode(this._parameters_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAMathFun(this);
    }

    public TProc getProc()
    {
        return this._proc_;
    }

    public void setProc(TProc node)
    {
        if(this._proc_ != null)
        {
            this._proc_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._proc_ = node;
    }

    public TId getId()
    {
        return this._id_;
    }

    public void setId(TId node)
    {
        if(this._id_ != null)
        {
            this._id_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._id_ = node;
    }

    public TLPar getLPar()
    {
        return this._lPar_;
    }

    public void setLPar(TLPar node)
    {
        if(this._lPar_ != null)
        {
            this._lPar_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._lPar_ = node;
    }

    public PExprRow getExpr()
    {
        return this._expr_;
    }

    public void setExpr(PExprRow node)
    {
        if(this._expr_ != null)
        {
            this._expr_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._expr_ = node;
    }

    public TComma getComma()
    {
        return this._comma_;
    }

    public void setComma(TComma node)
    {
        if(this._comma_ != null)
        {
            this._comma_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._comma_ = node;
    }

    public PColCsv getGroup()
    {
        return this._group_;
    }

    public void setGroup(PColCsv node)
    {
        if(this._group_ != null)
        {
            this._group_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._group_ = node;
    }

    public POptionsMap getParameters()
    {
        return this._parameters_;
    }

    public void setParameters(POptionsMap node)
    {
        if(this._parameters_ != null)
        {
            this._parameters_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._parameters_ = node;
    }

    public TRPar getRPar()
    {
        return this._rPar_;
    }

    public void setRPar(TRPar node)
    {
        if(this._rPar_ != null)
        {
            this._rPar_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._rPar_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._proc_)
            + toString(this._id_)
            + toString(this._lPar_)
            + toString(this._expr_)
            + toString(this._comma_)
            + toString(this._group_)
            + toString(this._parameters_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._proc_ == child)
        {
            this._proc_ = null;
            return;
        }

        if(this._id_ == child)
        {
            this._id_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._expr_ == child)
        {
            this._expr_ = null;
            return;
        }

        if(this._comma_ == child)
        {
            this._comma_ = null;
            return;
        }

        if(this._group_ == child)
        {
            this._group_ = null;
            return;
        }

        if(this._parameters_ == child)
        {
            this._parameters_ = null;
            return;
        }

        if(this._rPar_ == child)
        {
            this._rPar_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._proc_ == oldChild)
        {
            setProc((TProc) newChild);
            return;
        }

        if(this._id_ == oldChild)
        {
            setId((TId) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._expr_ == oldChild)
        {
            setExpr((PExprRow) newChild);
            return;
        }

        if(this._comma_ == oldChild)
        {
            setComma((TComma) newChild);
            return;
        }

        if(this._group_ == oldChild)
        {
            setGroup((PColCsv) newChild);
            return;
        }

        if(this._parameters_ == oldChild)
        {
            setParameters((POptionsMap) newChild);
            return;
        }

        if(this._rPar_ == oldChild)
        {
            setRPar((TRPar) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
