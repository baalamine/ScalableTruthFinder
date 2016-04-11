/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder.logical;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Update;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;
import org.qcri.truthdiscovery.util.dataModel.data.ValueBucket;


/**
 * @author mlba
 * @created date: Jan 26, 2016
 * 
 * @Input: KeyValuePair<Integer, List<ValueBucket>>
 * @Output: Cosine similarity between source trustworthiness scores
 */
public class FullyScalableTruthFinderUpdate extends Update<Double, KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6071548655967414404L;
	

	@SuppressWarnings("rawtypes")
	@Override
	public Double update(KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>> input, RheemContext context) 
	{
		// TODO Auto-generated method stub
		System.out.println("Update Transform") ;
		
		/**
		 * TODO
		 * @lamine ==> this method has to be re-implemented 
		 * Input: <SourceId, List<SourceClaim>>
		 * Procedure: 
		 * 					1. Update source trustworthiness
		 * 					2. compute source trustworthiness oscillation
		 * 
		 */
		return 0.0 ;
	}
	

	/**
	 * Compute the cosine similarity between the trustworthiness computed in the current iteration 
	 * and the trustworthiness computed in the previous iteration. 
	 * @return the cosine similarity
	 */
	public static double computeTrustworthinessCosineSimilarity(HashMap<String, Double> oldScores, HashMap<String, Double> newScores) {
		double a,b;
		double sumAB = 0;
		double sumA2=0;
		double sumB2 = 0;
		for (String s : oldScores.keySet()) {
			a = oldScores.get(s);
			b = newScores.get(s);
			sumAB = sumAB + (a*b);
			sumA2 = sumA2 + (a*a);
			sumB2 = sumB2 + (b*b);
		}
		sumA2 = Math.pow(sumA2, 0.5);
		sumB2 = Math.pow(sumB2, 0.5);
		if ((sumA2 * sumB2) == 0) {
			return Double.MAX_VALUE;
		}
		if (Double.isInfinite(sumAB)) {
			if (Double.isInfinite((sumA2 * sumB2))) {
				return 1.0;
			}
		}
		double cosineSimilarity = sumAB / (sumA2 * sumB2);
		return cosineSimilarity;
	}
	
	
}
