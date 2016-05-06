/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class AApiBlock extends PApiBlock
{
    private TApi _api_;
    private TId _engineName_;
    private TDot _dot_;
    private TId _insight_;
    private TLPar _lPar_;
    private PColCsv _selectors_;
    private PWhereStatement _where_;
    private PRelationClause _relations_;
    private TComma _comma_;
    private PMapObj _properties_;
    private TRPar _rPar_;

    public AApiBlock()
    {
        // Constructor
    }

    public AApiBlock(
        @SuppressWarnings("hiding") TApi _api_,
        @SuppressWarnings("hiding") TId _engineName_,
        @SuppressWarnings("hiding") TDot _dot_,
        @SuppressWarnings("hiding") TId _insight_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") PColCsv _selectors_,
        @SuppressWarnings("hiding") PWhereStatement _where_,
        @SuppressWarnings("hiding") PRelationClause _relations_,
        @SuppressWarnings("hiding") TComma _comma_,
        @SuppressWarnings("hiding") PMapObj _properties_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setApi(_api_);

        setEngineName(_engineName_);

        setDot(_dot_);

        setInsight(_insight_);

        setLPar(_lPar_);

        setSelectors(_selectors_);

        setWhere(_where_);

        setRelations(_relations_);

        setComma(_comma_);

        setProperties(_properties_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new AApiBlock(
            cloneNode(this._api_),
            cloneNode(this._engineName_),
            cloneNode(this._dot_),
            cloneNode(this._insight_),
            cloneNode(this._lPar_),
            cloneNode(this._selectors_),
            cloneNode(this._where_),
            cloneNode(this._relations_),
            cloneNode(this._comma_),
            cloneNode(this._properties_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAApiBlock(this);
    }

    public TApi getApi()
    {
        return this._api_;
    }

    public void setApi(TApi node)
    {
        if(this._api_ != null)
        {
            this._api_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._api_ = node;
    }

    public TId getEngineName()
    {
        return this._engineName_;
    }

    public void setEngineName(TId node)
    {
        if(this._engineName_ != null)
        {
            this._engineName_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._engineName_ = node;
    }

    public TDot getDot()
    {
        return this._dot_;
    }

    public void setDot(TDot node)
    {
        if(this._dot_ != null)
        {
            this._dot_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._dot_ = node;
    }

    public TId getInsight()
    {
        return this._insight_;
    }

    public void setInsight(TId node)
    {
        if(this._insight_ != null)
        {
            this._insight_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._insight_ = node;
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

    public PColCsv getSelectors()
    {
        return this._selectors_;
    }

    public void setSelectors(PColCsv node)
    {
        if(this._selectors_ != null)
        {
            this._selectors_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._selectors_ = node;
    }

    public PWhereStatement getWhere()
    {
        return this._where_;
    }

    public void setWhere(PWhereStatement node)
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

    public PRelationClause getRelations()
    {
        return this._relations_;
    }

    public void setRelations(PRelationClause node)
    {
        if(this._relations_ != null)
        {
            this._relations_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._relations_ = node;
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

    public PMapObj getProperties()
    {
        return this._properties_;
    }

    public void setProperties(PMapObj node)
    {
        if(this._properties_ != null)
        {
            this._properties_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._properties_ = node;
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
            + toString(this._api_)
            + toString(this._engineName_)
            + toString(this._dot_)
            + toString(this._insight_)
            + toString(this._lPar_)
            + toString(this._selectors_)
            + toString(this._where_)
            + toString(this._relations_)
            + toString(this._comma_)
            + toString(this._properties_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._api_ == child)
        {
            this._api_ = null;
            return;
        }

        if(this._engineName_ == child)
        {
            this._engineName_ = null;
            return;
        }

        if(this._dot_ == child)
        {
            this._dot_ = null;
            return;
        }

        if(this._insight_ == child)
        {
            this._insight_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._selectors_ == child)
        {
            this._selectors_ = null;
            return;
        }

        if(this._where_ == child)
        {
            this._where_ = null;
            return;
        }

        if(this._relations_ == child)
        {
            this._relations_ = null;
            return;
        }

        if(this._comma_ == child)
        {
            this._comma_ = null;
            return;
        }

        if(this._properties_ == child)
        {
            this._properties_ = null;
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
        if(this._api_ == oldChild)
        {
            setApi((TApi) newChild);
            return;
        }

        if(this._engineName_ == oldChild)
        {
            setEngineName((TId) newChild);
            return;
        }

        if(this._dot_ == oldChild)
        {
            setDot((TDot) newChild);
            return;
        }

        if(this._insight_ == oldChild)
        {
            setInsight((TId) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._selectors_ == oldChild)
        {
            setSelectors((PColCsv) newChild);
            return;
        }

        if(this._where_ == oldChild)
        {
            setWhere((PWhereStatement) newChild);
            return;
        }

        if(this._relations_ == oldChild)
        {
            setRelations((PRelationClause) newChild);
            return;
        }

        if(this._comma_ == oldChild)
        {
            setComma((TComma) newChild);
            return;
        }

        if(this._properties_ == oldChild)
        {
            setProperties((PMapObj) newChild);
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
