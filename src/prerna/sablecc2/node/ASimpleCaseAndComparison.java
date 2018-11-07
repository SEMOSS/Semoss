/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import java.util.*;
import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class ASimpleCaseAndComparison extends PAndComparison
{
    private PTerm _left_;
    private TAndComparator _andComparator_;
    private PTerm _right_;
    private final LinkedList<PRepeatingAndComparison> _moreRight_ = new LinkedList<PRepeatingAndComparison>();

    public ASimpleCaseAndComparison()
    {
        // Constructor
    }

    public ASimpleCaseAndComparison(
        @SuppressWarnings("hiding") PTerm _left_,
        @SuppressWarnings("hiding") TAndComparator _andComparator_,
        @SuppressWarnings("hiding") PTerm _right_,
        @SuppressWarnings("hiding") List<?> _moreRight_)
    {
        // Constructor
        setLeft(_left_);

        setAndComparator(_andComparator_);

        setRight(_right_);

        setMoreRight(_moreRight_);

    }

    @Override
    public Object clone()
    {
        return new ASimpleCaseAndComparison(
            cloneNode(this._left_),
            cloneNode(this._andComparator_),
            cloneNode(this._right_),
            cloneList(this._moreRight_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseASimpleCaseAndComparison(this);
    }

    public PTerm getLeft()
    {
        return this._left_;
    }

    public void setLeft(PTerm node)
    {
        if(this._left_ != null)
        {
            this._left_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._left_ = node;
    }

    public TAndComparator getAndComparator()
    {
        return this._andComparator_;
    }

    public void setAndComparator(TAndComparator node)
    {
        if(this._andComparator_ != null)
        {
            this._andComparator_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._andComparator_ = node;
    }

    public PTerm getRight()
    {
        return this._right_;
    }

    public void setRight(PTerm node)
    {
        if(this._right_ != null)
        {
            this._right_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._right_ = node;
    }

    public LinkedList<PRepeatingAndComparison> getMoreRight()
    {
        return this._moreRight_;
    }

    public void setMoreRight(List<?> list)
    {
        for(PRepeatingAndComparison e : this._moreRight_)
        {
            e.parent(null);
        }
        this._moreRight_.clear();

        for(Object obj_e : list)
        {
            PRepeatingAndComparison e = (PRepeatingAndComparison) obj_e;
            if(e.parent() != null)
            {
                e.parent().removeChild(e);
            }

            e.parent(this);
            this._moreRight_.add(e);
        }
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._left_)
            + toString(this._andComparator_)
            + toString(this._right_)
            + toString(this._moreRight_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._left_ == child)
        {
            this._left_ = null;
            return;
        }

        if(this._andComparator_ == child)
        {
            this._andComparator_ = null;
            return;
        }

        if(this._right_ == child)
        {
            this._right_ = null;
            return;
        }

        if(this._moreRight_.remove(child))
        {
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._left_ == oldChild)
        {
            setLeft((PTerm) newChild);
            return;
        }

        if(this._andComparator_ == oldChild)
        {
            setAndComparator((TAndComparator) newChild);
            return;
        }

        if(this._right_ == oldChild)
        {
            setRight((PTerm) newChild);
            return;
        }

        for(ListIterator<PRepeatingAndComparison> i = this._moreRight_.listIterator(); i.hasNext();)
        {
            if(i.next() == oldChild)
            {
                if(newChild != null)
                {
                    i.set((PRepeatingAndComparison) newChild);
                    newChild.parent(this);
                    oldChild.parent(null);
                    return;
                }

                i.remove();
                oldChild.parent(null);
                return;
            }
        }

        throw new RuntimeException("Not a child.");
    }
}
