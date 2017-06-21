/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class ADataModel extends PDataModel
{
    private TDatamodeltoken _datamodeltoken_;
    private TLPar _lPar_;
    private TJsonblock _json_;
    private TRPar _rPar_;

    public ADataModel()
    {
        // Constructor
    }

    public ADataModel(
        @SuppressWarnings("hiding") TDatamodeltoken _datamodeltoken_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") TJsonblock _json_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setDatamodeltoken(_datamodeltoken_);

        setLPar(_lPar_);

        setJson(_json_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new ADataModel(
            cloneNode(this._datamodeltoken_),
            cloneNode(this._lPar_),
            cloneNode(this._json_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseADataModel(this);
    }

    public TDatamodeltoken getDatamodeltoken()
    {
        return this._datamodeltoken_;
    }

    public void setDatamodeltoken(TDatamodeltoken node)
    {
        if(this._datamodeltoken_ != null)
        {
            this._datamodeltoken_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._datamodeltoken_ = node;
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

    public TJsonblock getJson()
    {
        return this._json_;
    }

    public void setJson(TJsonblock node)
    {
        if(this._json_ != null)
        {
            this._json_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._json_ = node;
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
            + toString(this._datamodeltoken_)
            + toString(this._lPar_)
            + toString(this._json_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._datamodeltoken_ == child)
        {
            this._datamodeltoken_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._json_ == child)
        {
            this._json_ = null;
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
        if(this._datamodeltoken_ == oldChild)
        {
            setDatamodeltoken((TDatamodeltoken) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._json_ == oldChild)
        {
            setJson((TJsonblock) newChild);
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
