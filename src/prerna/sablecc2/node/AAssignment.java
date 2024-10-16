/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AAssignment extends PAssignment
{
    private TId _id_;
    private TEqual _equal_;
    private PScript _script_;

    public AAssignment()
    {
        // Constructor
    }

    public AAssignment(
        @SuppressWarnings("hiding") TId _id_,
        @SuppressWarnings("hiding") TEqual _equal_,
        @SuppressWarnings("hiding") PScript _script_)
    {
        // Constructor
        setId(_id_);

        setEqual(_equal_);

        setScript(_script_);

    }

    @Override
    public Object clone()
    {
        return new AAssignment(
            cloneNode(this._id_),
            cloneNode(this._equal_),
            cloneNode(this._script_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAAssignment(this);
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

    public TEqual getEqual()
    {
        return this._equal_;
    }

    public void setEqual(TEqual node)
    {
        if(this._equal_ != null)
        {
            this._equal_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._equal_ = node;
    }

    public PScript getScript()
    {
        return this._script_;
    }

    public void setScript(PScript node)
    {
        if(this._script_ != null)
        {
            this._script_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._script_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._id_)
            + toString(this._equal_)
            + toString(this._script_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._id_ == child)
        {
            this._id_ = null;
            return;
        }

        if(this._equal_ == child)
        {
            this._equal_ = null;
            return;
        }

        if(this._script_ == child)
        {
            this._script_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._id_ == oldChild)
        {
            setId((TId) newChild);
            return;
        }

        if(this._equal_ == oldChild)
        {
            setEqual((TEqual) newChild);
            return;
        }

        if(this._script_ == oldChild)
        {
            setScript((PScript) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
