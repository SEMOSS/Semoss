/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class AFilterColumn extends PFilterColumn
{
    private TColfilter _colfilter_;
    private TLPar _lPar_;
    private PWhereClause _where_;
    private TRPar _rPar_;

    public AFilterColumn()
    {
        // Constructor
    }

    public AFilterColumn(
        @SuppressWarnings("hiding") TColfilter _colfilter_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") PWhereClause _where_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setColfilter(_colfilter_);

        setLPar(_lPar_);

        setWhere(_where_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new AFilterColumn(
            cloneNode(this._colfilter_),
            cloneNode(this._lPar_),
            cloneNode(this._where_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAFilterColumn(this);
    }

    public TColfilter getColfilter()
    {
        return this._colfilter_;
    }

    public void setColfilter(TColfilter node)
    {
        if(this._colfilter_ != null)
        {
            this._colfilter_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._colfilter_ = node;
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

    public PWhereClause getWhere()
    {
        return this._where_;
    }

    public void setWhere(PWhereClause node)
    {
        if(this._where_ != null)
        {
            this._where_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._where_ = node;
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
            + toString(this._colfilter_)
            + toString(this._lPar_)
            + toString(this._where_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._colfilter_ == child)
        {
            this._colfilter_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._where_ == child)
        {
            this._where_ = null;
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
        if(this._colfilter_ == oldChild)
        {
            setColfilter((TColfilter) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._where_ == oldChild)
        {
            setWhere((PWhereClause) newChild);
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
