/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class APanelModelPanelop extends PPanelop
{
    private PPanelModel _panelModel_;

    public APanelModelPanelop()
    {
        // Constructor
    }

    public APanelModelPanelop(
        @SuppressWarnings("hiding") PPanelModel _panelModel_)
    {
        // Constructor
        setPanelModel(_panelModel_);

    }

    @Override
    public Object clone()
    {
        return new APanelModelPanelop(
            cloneNode(this._panelModel_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAPanelModelPanelop(this);
    }

    public PPanelModel getPanelModel()
    {
        return this._panelModel_;
    }

    public void setPanelModel(PPanelModel node)
    {
        if(this._panelModel_ != null)
        {
            this._panelModel_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._panelModel_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._panelModel_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._panelModel_ == child)
        {
            this._panelModel_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._panelModel_ == oldChild)
        {
            setPanelModel((PPanelModel) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
