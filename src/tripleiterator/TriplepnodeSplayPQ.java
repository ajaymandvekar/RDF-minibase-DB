package tripleiterator;

import global.*;
import java.io.*;

/**
 * Implements a sorted binary tree (extends class TriplepnodePQ).
 * Implements the <code>enq</code> and the <code>deq</code> functions.
 */
public class TriplepnodeSplayPQ extends TriplepnodePQ
{

	/** the root of the tree */
	protected TriplepnodeSplayNode   root;

	/**
	 * class constructor, sets default values.
	 */
	public TriplepnodeSplayPQ() 
	{
		root = null;
		count = 0;
		sort_order = new TripleOrder(TripleOrder.SubjectPredicateObjectConfidence); //XXX TODO AJAY:Check TripleOrder
	}

	/**
	 * class constructor.
	 * @param order   the order of sorting (Ascending or Descending)
	 */  
	public TriplepnodeSplayPQ(TripleOrder order)
	{
		root = null;
		count = 0;
		sort_order = order;
	}

	/**
	 * Inserts an element into the binary tree.
	 * @param item the element to be inserted
	 * @exception IOException from lower layers
	 * @exception TripleUtilsException error in tuple compare routines
	 */
	public void Tripleenq(Triplepnode item) throws
		IOException, UnknowAttrType, TripleUtilsException 
		{
			count ++;
			TriplepnodeSplayNode newnode = new TriplepnodeSplayNode(item);
			TriplepnodeSplayNode t = root;

			if (t == null) 
			{
				root = newnode;
				return;
			}

			int comp = TriplepnodeCMP(item, t.item);

			TriplepnodeSplayNode l = TriplepnodeSplayNode.dummy;
			TriplepnodeSplayNode r = TriplepnodeSplayNode.dummy;

			boolean done = false;

			while (!done) 
			{
				if ((comp >= 0)) 
				{
					TriplepnodeSplayNode tr = t.rt;
					if (tr == null) 
					{
						tr = newnode;
						comp = 0;
						done = true;
					}
					else
					{ 
						comp = TriplepnodeCMP(item, tr.item);
					}

					if ((comp <= 0))  
					{
						l.rt = t; t.par = l;
						l = t;
						t = tr;
					}
					else 
					{
						TriplepnodeSplayNode trr = tr.rt;
						if (trr == null) 
						{
							trr = newnode;
							comp = 0;
							done = true;
						}
						else
						{

							comp = TriplepnodeCMP(item, trr.item);
						}

						if ((t.rt = tr.lt) != null) 
						{
							t.rt.par = t;
						}
						tr.lt = t; t.par = tr;
						l.rt = tr; tr.par = l;
						l = tr;
						t = trr;
					}
				} // end of if(comp >= 0)
				else 
				{
					TriplepnodeSplayNode tl = t.lt;
					if (tl == null) 
					{
						tl = newnode;
						comp = 0;
						done = true;
					}
					else
					{
						comp = TriplepnodeCMP(item, tl.item);
					}

					if ((comp >= 0)) 
					{
						r.lt = t; t.par = r;
						r = t;
						t = tl;
					}
					else 
					{
						TriplepnodeSplayNode tll = tl.lt;
						if (tll == null) 
						{
							tll = newnode;
							comp = 0;
							done = true;
						}
						else
						{
							comp = TriplepnodeCMP(item, tll.item);
						}

						if ((t.lt = tl.rt) != null)
						{
							t.lt.par = t;
						}
						tl.rt = t; t.par = tl;
						r.lt = tl; tl.par = r;
						r = tl;
						t = tll;
					}
				} // end of else
			} // end of while(!done)

			if ((r.lt = t.rt) != null) r.lt.par = r;
			if ((l.rt = t.lt) != null) l.rt.par = l;
			if ((t.lt = TriplepnodeSplayNode.dummy.rt) != null) t.lt.par = t;
			if ((t.rt = TriplepnodeSplayNode.dummy.lt) != null) t.rt.par = t;
			t.par = null;
			root = t;

			return; 
		}

	/**
	 * Removes the minimum (Ascending)
	 * @return the element removed
	 */
	public Triplepnode Tripledeq() 
	{
		if (root == null) return null;

		count --;
		TriplepnodeSplayNode t = root;
		TriplepnodeSplayNode l = root.lt;
		if (l == null) 
		{
			if ((root = t.rt) != null) root.par = null;
			return t.item;
		}
		else 
		{
			while (true) 
			{
				TriplepnodeSplayNode ll = l.lt;
				if (ll == null) 
				{
					if ((t.lt = l.rt) != null) t.lt.par = t;
					return l.item;
				}
				else 
				{
					TriplepnodeSplayNode lll = ll.lt;
					if (lll == null) 
					{
						if((l.lt = ll.rt) != null) l.lt.par = l;
						return ll.item;
					}
					else 
					{
						t.lt = ll; ll.par = t;
						if ((l.lt = ll.rt) != null) l.lt.par = l;
						ll.rt = l; l.par = ll;
						t = ll;
						l = lll;
					}
				}
			} // end of while(true)
		} 
	}

}
