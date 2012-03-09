package global;

/** 
 * Enumeration class for TripleOrder
 * 
 */

public class TripleOrder {

	public static final int SubjectPredicateObjectConfidence = 1;
	public static final int PredicateSubjectObjectConfidence = 2;
	public static final int SubjectConfidence = 3;
	public static final int PredicateConfidence = 4;
	public static final int ObjectConfidence = 5;
	public static final int Confidence = 6;

	public int tripleOrder;

	/** 
	 * TripleOrder Constructor
	 * <br>
	 * A triple ordering can be defined as 
	 * <ul>
	 * <li>   TripleOrder tripleOrder = new TripleOrder(TripleOrder.Confidence);
	 * </ul>
	 * and subsequently used as
	 * <ul>
	 * <li>   if (tripleOrder.tripleOrder == TripleOrder.Confidence) ....
	 * </ul>
	 *
	 * @param _tripleOrder The possible sorting orderType of the triples 
	 */

	public TripleOrder (int _tripleOrder) 
	{
		tripleOrder = _tripleOrder;
	}

	public String toString() 
	{
		switch (tripleOrder) 
		{
			case SubjectPredicateObjectConfidence:
				return "SubjectPredicateObjectConfidence";
			case PredicateSubjectObjectConfidence:
				return "PredicateSubjectObjectConfidence";
			case SubjectConfidence:
				return "SubjectConfidence";
			case PredicateConfidence:
				return "PredicateConfidence";
			case ObjectConfidence:
				return "ObjectConfidence";
			case Confidence:
				return "Confidence";
		}
		return ("Unexpected TripleOrder " + tripleOrder);
	}

}
