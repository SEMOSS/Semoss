/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class APanelConfig extends PPanelConfig
{
    private TPanelconfig _panelconfig_;
    private TLPar _lPar_;
    private PMapObj _map_;
    private TRPar _rPar_;

    public APanelConfig()
    {
        // Constructor
    }

    public APanelConfig(
        @SuppressWarnings("hiding") TPanelconfig _panelconfig_,
        @SuppressWarnings("hiding") TLPar _lPar_,
        @SuppressWarnings("hiding") PMapObj _map_,
        @SuppressWarnings("hiding") TRPar _rPar_)
    {
        // Constructor
        setPanelconfig(_panelconfig_);

        setLPar(_lPar_);

        setMap(_map_);

        setRPar(_rPar_);

    }

    @Override
    public Object clone()
    {
        return new APanelConfig(
            cloneNode(this._panelconfig_),
            cloneNode(this._lPar_),
            cloneNode(this._map_),
            cloneNode(this._rPar_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPanelConfig(this);
    }

    public TPanelconfig getPanelconfig()
    {
        return this._panelconfig_;
    }

    public void setPanelconfig(TPanelconfig node)
    {
        if(this._panelconfig_ != null)
        {
            this._panelconfig_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._panelconfig_ = node;
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

    public PMapObj getMap()
    {
        return this._map_;
    }

    public void setMap(PMapObj node)
    {
        if(this._map_ != null)
        {
            this._map_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._map_ = node;
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
            + toString(this._panelconfig_)
            + toString(this._lPar_)
            + toString(this._map_)
            + toString(this._rPar_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._panelconfig_ == child)
        {
            this._panelconfig_ = null;
            return;
        }

        if(this._lPar_ == child)
        {
            this._lPar_ = null;
            return;
        }

        if(this._map_ == child)
        {
            this._map_ = null;
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
        if(this._panelconfig_ == oldChild)
        {
            setPanelconfig((TPanelconfig) newChild);
            return;
        }

        if(this._lPar_ == oldChild)
        {
            setLPar((TLPar) newChild);
            return;
        }

        if(this._map_ == oldChild)
        {
            setMap((PMapObj) newChild);
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
