/**
 * 
 */
package qcri.truthdiscovery.fullyscalable.truthfinder.logical;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.ml4all.logical.Update;

import qcri.dafna.dataModel.data.SourceClaim;
import qcri.dafna.dataModel.data.ValueBucket;


/**
 * @author mlba
 * @created date: Jan 26, 2016
 * 
 * @Input: KeyValuePair<Integer, List<ValueBucket>>
 * @Output: Cosine similarity between source trustworthiness scores
 */
public class FullyScalableTruthFinderUpdate extends Update<Double, KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6071548655967414404L;
	

	@SuppressWarnings("rawtypes")
	@Override
	public Double update(KeyValuePair<Integer, KeyValuePair<Integer, List<ValueBucket>>> input, RheemContext context) 
	{
		// TODO Auto-generated method stub
		System.out.println("Update Transform") ;
		double cosine = 0 ;
		@SuppressWarnings("unchecked")
		ArrayList<Double> params = (ArrayList<Double>) context.getByKey("params") ;
 		double dampingFactor = params.get(2) ;
		
		//Update trust scores
		HashMap<String, ArrayList<Double>> tmap = new HashMap<String, ArrayList<Double>>() ;
		//System.out.println(input.getClass().getName()) ;
		//System.out.println(input.value.getClass().getName()) ;
		//System.out.println(input.key);
		ArrayList<ValueBucket> buckets = (ArrayList<ValueBucket>) input.value.value ;
		for (ValueBucket bucketList: buckets)
		{
			for (SourceClaim s : bucketList.getClaims())
			{
				double vote = (1 - Math.exp((-1 * dampingFactor) * s.getBucket().getConfidence())) ;
				if (tmap.keySet().contains(s.getSource().getSourceIdentifier()))
					tmap.get(s.getSource().getSourceIdentifier()).add( vote ) ;
				else
				{
					ArrayList<Double> values = new ArrayList<Double> () ;
					values.add( vote ) ;
					tmap.put( s.getSource().getSourceIdentifier(), values ) ;
				}
			}
		}
		//compute trust scores
		HashMap<String, Double>  newscores = new HashMap<String, Double>() ;
		for (String s : tmap.keySet())
		{
			double sum = 0 ;
			for (double val : tmap.get(s)) sum = sum + val ;
			sum = sum / tmap.get(s).size() ;
			
			newscores.put(s, sum) ; //save
		}
	
		//compute the cosine
		ArrayList<String> previous_trust = (ArrayList<String>) context.getByKey("trustworthiness") ;
		HashMap<String, Double> previous_scores = new HashMap<String, Double>() ;
		
		if (previous_trust.isEmpty())
		{
			for (String s : newscores.keySet())
				previous_scores.put(s, 0.8) ;
				
		}
		else
		{
			for (String entry: previous_trust)
			{
				String[] fields = entry.split("===>") ;
				previous_scores.put(fields[0], Double.parseDouble(fields[1])) ;
			}
			previous_trust.clear();  
		}
		cosine = computeTrustworthinessCosineSimilarity(previous_scores, newscores) ;
		
		//Update trust global variable and push
		for (String s : newscores.keySet()) previous_trust.add(s+ "===>" + newscores.get(s)) ;
 		context.push("trustworthiness", previous_trust) ;
		
		//Push params global variables
		context.push("params", params) ;
		
		return cosine ;
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
