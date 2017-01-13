/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AAsop extends PAsop
{
    private TAsOp _asOp_;
    private TLPar _lPar_;
    private PGenRow _genRow_;
    private TRPar _rPar_;

    public AAsop()
    {
        // Constructor
    }

    public AAsop(
        @SuppressWarnings("hiding") TAsOp _asOp_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") PGenRow _genRow_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setAsOp(_asOp_);

        setLPar(_lPar_);

        setGenRow(_genRow_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new AAsop(
            cloneNode(this._asOp_),
            cloneNode(this._lPar_),
            cloneNode(this._genRow_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAAsop(this);
    }

    public TAsOp getAsOp()
    {
        return this._asOp_;
    }

    public void setAsOp(TAsOp node)
    {
        if(this._asOp_ != null)
        {
            this._asOp_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._asOp_ = node;
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

    public PGenRow getGenRow()
    {
        return this._genRow_;
    }

    public void setGenRow(PGenRow node)
    {
        if(this._genRow_ != null)
        {
            this._genRow_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._genRow_ = node;
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
            + toString(this._asOp_)
            + toString(this._lPar_)
            + toString(this._genRow_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._asOp_ == child)
        {
            this._asOp_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._genRow_ == child)
        {
            this._genRow_ = null;
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
        if(this._asOp_ == oldChild)
        {
            setAsOp((TAsOp) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._genRow_ == oldChild)
        {
            setGenRow((PGenRow) newChild);
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
