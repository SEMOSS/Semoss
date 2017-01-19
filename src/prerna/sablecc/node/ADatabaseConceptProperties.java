/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class ADatabaseConceptProperties extends PDatabaseConceptProperties
{
    private TDatabaseconceptpropertiesToken _databaseconceptpropertiesToken_;
    private TLPar _lPar_;
    private TId _conceptName_;
    private TComma _comma_;
    private TId _engineName_;
    private TRPar _rPar_;

    public ADatabaseConceptProperties()
    {
        // Constructor
    }

    public ADatabaseConceptProperties(
        @SuppressWarnings("hiding") TDatabaseconceptpropertiesToken _databaseconceptpropertiesToken_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") TId _conceptName_,
        @SuppressWarnings("hiding") TComma _comma_,
        @SuppressWarnings("hiding") TId _engineName_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setDatabaseconceptpropertiesToken(_databaseconceptpropertiesToken_);

        setLPar(_lPar_);

        setConceptName(_conceptName_);

        setComma(_comma_);

        setEngineName(_engineName_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new ADatabaseConceptProperties(
            cloneNode(this._databaseconceptpropertiesToken_),
            cloneNode(this._lPar_),
            cloneNode(this._conceptName_),
            cloneNode(this._comma_),
            cloneNode(this._engineName_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseADatabaseConceptProperties(this);
    }

    public TDatabaseconceptpropertiesToken getDatabaseconceptpropertiesToken()
    {
        return this._databaseconceptpropertiesToken_;
    }

    public void setDatabaseconceptpropertiesToken(TDatabaseconceptpropertiesToken node)
    {
        if(this._databaseconceptpropertiesToken_ != null)
        {
            this._databaseconceptpropertiesToken_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._databaseconceptpropertiesToken_ = node;
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

    public TId getConceptName()
    {
        return this._conceptName_;
    }

    public void setConceptName(TId node)
    {
        if(this._conceptName_ != null)
        {
            this._conceptName_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._conceptName_ = node;
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
            + toString(this._databaseconceptpropertiesToken_)
            + toString(this._lPar_)
            + toString(this._conceptName_)
            + toString(this._comma_)
            + toString(this._engineName_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._databaseconceptpropertiesToken_ == child)
        {
            this._databaseconceptpropertiesToken_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._conceptName_ == child)
        {
            this._conceptName_ = null;
            return;
        }

        if(this._comma_ == child)
        {
            this._comma_ = null;
            return;
        }

        if(this._engineName_ == child)
        {
            this._engineName_ = null;
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
        if(this._databaseconceptpropertiesToken_ == oldChild)
        {
            setDatabaseconceptpropertiesToken((TDatabaseconceptpropertiesToken) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._conceptName_ == oldChild)
        {
            setConceptName((TId) newChild);
            return;
        }

        if(this._comma_ == oldChild)
        {
            setComma((TComma) newChild);
            return;
        }

        if(this._engineName_ == oldChild)
        {
            setEngineName((TId) newChild);
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
