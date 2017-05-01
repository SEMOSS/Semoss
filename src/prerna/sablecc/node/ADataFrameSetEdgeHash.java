/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class ADataFrameSetEdgeHash extends PDataFrameSetEdgeHash
{
    private TDataframesetedgehash _dataframesetedgehash_;
    private TLPar _lPar_;
    private PWordOrNum _wordOrNum_;
    private TRPar _rPar_;

    public ADataFrameSetEdgeHash()
    {
        // Constructor
    }

    public ADataFrameSetEdgeHash(
        @SuppressWarnings("hiding") TDataframesetedgehash _dataframesetedgehash_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") PWordOrNum _wordOrNum_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setDataframesetedgehash(_dataframesetedgehash_);

        setLPar(_lPar_);

        setWordOrNum(_wordOrNum_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new ADataFrameSetEdgeHash(
            cloneNode(this._dataframesetedgehash_),
            cloneNode(this._lPar_),
            cloneNode(this._wordOrNum_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseADataFrameSetEdgeHash(this);
    }

    public TDataframesetedgehash getDataframesetedgehash()
    {
        return this._dataframesetedgehash_;
    }

    public void setDataframesetedgehash(TDataframesetedgehash node)
    {
        if(this._dataframesetedgehash_ != null)
        {
            this._dataframesetedgehash_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._dataframesetedgehash_ = node;
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

    public PWordOrNum getWordOrNum()
    {
        return this._wordOrNum_;
    }

    public void setWordOrNum(PWordOrNum node)
    {
        if(this._wordOrNum_ != null)
        {
            this._wordOrNum_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._wordOrNum_ = node;
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
            + toString(this._dataframesetedgehash_)
            + toString(this._lPar_)
            + toString(this._wordOrNum_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._dataframesetedgehash_ == child)
        {
            this._dataframesetedgehash_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._wordOrNum_ == child)
        {
            this._wordOrNum_ = null;
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
        if(this._dataframesetedgehash_ == oldChild)
        {
            setDataframesetedgehash((TDataframesetedgehash) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._wordOrNum_ == oldChild)
        {
            setWordOrNum((PWordOrNum) newChild);
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
