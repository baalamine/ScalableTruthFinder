/**
 * 
 */
package qcri.truthdiscovery.scalable.truthfinder.logical;

import java.util.List;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.ml4all.logical.Loop;

/**
 * @author mlba
 * @created date: Jan 26, 2016
 * Scalable Truth Finder Loop
 * 
 * @Input: 
 * @Output: 
 */
public class ScalableTruthFinderLoop extends Loop<List<Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6073417690402864194L;
	double cosineThresold ;

	public ScalableTruthFinderLoop(double cosineThresold) 
	{
		// TODO Auto-generated constructor stub
		this.cosineThresold = cosineThresold ;
	}

	@Override
	public boolean condition(List<Double> cosineSimilarity, RheemContext context) {
		// TODO Auto-generated method stub
		System.out.println("LoopOp") ;
		
		//System.out.println("Array Size: " + cosineSimilarity.size()) ;
		double cosine = cosineSimilarity.get(0) ;
		//System.out.println(cosine) ;
		
		if ( 1 - cosine > this.cosineThresold ) return true; //process continues
		else return false ; //we are done.
	}

}
