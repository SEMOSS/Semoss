/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class APanelSetBuilder extends PPanelSetBuilder
{
    private TPanelsetbuilder _panelsetbuilder_;
    private TLPar _lPar_;
    private TWord _builder_;
    private TRPar _rPar_;

    public APanelSetBuilder()
    {
        // Constructor
    }

    public APanelSetBuilder(
        @SuppressWarnings("hiding") TPanelsetbuilder _panelsetbuilder_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") TWord _builder_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setPanelsetbuilder(_panelsetbuilder_);

        setLPar(_lPar_);

        setBuilder(_builder_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new APanelSetBuilder(
            cloneNode(this._panelsetbuilder_),
            cloneNode(this._lPar_),
            cloneNode(this._builder_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPanelSetBuilder(this);
    }

    public TPanelsetbuilder getPanelsetbuilder()
    {
        return this._panelsetbuilder_;
    }

    public void setPanelsetbuilder(TPanelsetbuilder node)
    {
        if(this._panelsetbuilder_ != null)
        {
            this._panelsetbuilder_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._panelsetbuilder_ = node;
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

    public TWord getBuilder()
    {
        return this._builder_;
    }

    public void setBuilder(TWord node)
    {
        if(this._builder_ != null)
        {
            this._builder_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._builder_ = node;
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
            + toString(this._panelsetbuilder_)
            + toString(this._lPar_)
            + toString(this._builder_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._panelsetbuilder_ == child)
        {
            this._panelsetbuilder_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._builder_ == child)
        {
            this._builder_ = null;
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
        if(this._panelsetbuilder_ == oldChild)
        {
            setPanelsetbuilder((TPanelsetbuilder) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._builder_ == oldChild)
        {
            setBuilder((TWord) newChild);
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
