/**
 * 
 */
package org.qcri.truthdiscovery.truthfinder.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Compute;
import org.qcri.truthdiscovery.util.dataModel.data.DataSet;
import org.qcri.truthdiscovery.util.dataModel.data.Source;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;
import org.qcri.truthdiscovery.util.dataModel.data.ValueBucket;
import org.qcri.truthdiscovery.util.dataModel.dataFormatter.DataComparator;

/**
 * @author mlba
 * @created date: december 10, 2016
 * @desc: Truth Finder Compute Logical Function
 */
public class TruthFinderCompute extends Compute<KeyValuePair<Integer, org.qcri.truthdiscovery.util.dataModel.data.DataSet>, KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>>> {

	/**
	 * 
	 */
	DataSet kv = new DataSet() ;
	
	@SuppressWarnings("unchecked")
	@Override
	public KeyValuePair<Integer, org.qcri.truthdiscovery.util.dataModel.data.DataSet> compute(KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>>  input, RheemContext context) {
		// TODO Auto-generated method stub
		System.out.println("Compute Operator") ;	
		
		//Initialization phase	
		System.out.println ("Numùber of claim ==> "+input.value.key) ;
		for (SourceClaim sc : input.value.value)
			kv.addClaim(sc.getId(), sc.getObjectIdentifier(), "", sc.getPropertyName(), 
							sc.getPropertyValueString(), sc.getWeight(), sc.getTimeStamp(), sc.getSource().getSourceIdentifier()) ;
		kv.computeValueBuckets(false);
		
		ArrayList<String> trustworthinessScores =  (ArrayList<String>) context.getByKey("trustworthiness") ;
		HashMap<String, Double> tmap = new HashMap<String, Double> () ;
		if ( ! trustworthinessScores.isEmpty() )
		{
			System.out.println("OKOKOK") ;
			for (String entry: trustworthinessScores)
			{
				String[] fields = entry.split("===>") ;
				String k = fields[0] ;
				double score = Double.parseDouble(fields[1]) ;
				tmap.put(k, score) ;
			}
		}
		
		/**
		 * Computation phase
		 */
		ArrayList<Double> params = (ArrayList<Double>) context.getByKey("params") ;
		double similarityConstant = params.get(0) ;
		double base_sim = params.get(1) ;
		double dampingFactor = params.get(2) ;
				
		computeConfidenceScore(tmap) ; //confidence wo similarity
		computeConfidenceScoreWithSimilarity(similarityConstant, base_sim) ;  //confidence with similarity
		computeConfidence(dampingFactor) ; //final confidence value
		computeTrustworthiness(dampingFactor); //trustworthiness value
				
		return new KeyValuePair<Integer, org.qcri.truthdiscovery.util.dataModel.data.DataSet>(1, kv) ;
	}

	/**
	 * compute the confidence score of the claims without mention 
	 * for the claim similarity.
	 * this method runs first, then the @computeConfidenceWithSimilarity method 
	 * is run to enhance the confidence computation.
	 */
	private void computeConfidenceScore(HashMap<String, Double> sourceTrustworthiness) 
	{
		double ln = 0;
		double lnSum = 0;
		
		for (List<ValueBucket> backetsList: kv.getDataItemsBuckets().values()) {
			/* 
			 * All claims, from different sources, for single property value.
			 */
			for (ValueBucket bucket : backetsList) {
				lnSum = 0;
				for (Source source : bucket.getSources()) {
					
					if (sourceTrustworthiness.isEmpty()) //initial trust
					{
						ln    = Math.log(1 - source.getTrustworthiness());
						lnSum = lnSum - ln;
					}
					else
					{
						ln = Math.log(1 - sourceTrustworthiness.get(source.getSourceIdentifier())) ; //up-to-date trust
						lnSum = lnSum - ln;
					}
				}
				bucket.setConfidence(lnSum);
			}
		}
	}

	
	/**
	 * Compute the confidence score of a claim based on its already computed confidence score
	 * and the similarity between it and the other claims.
	 * Then set the confidence value to this new confidence computed with similarity measure. 
	 */
	private void computeConfidenceScoreWithSimilarity(double similarityConstant, double base_sim) {
		double similarity;
		double similaritySum;
		for (List<ValueBucket> bucketList : kv.getDataItemsBuckets().values()) {
			for (ValueBucket bucket1 : bucketList) {
				similaritySum = 0;
				for (ValueBucket bucket2 : bucketList) {
					// test if same bucket and continue
					if (bucket1.getClaims().get(0).getId() == bucket2.getClaims().get(0).getId()) {
						continue;
					}
					similarity = computeClaimsSimilarity(bucket1, bucket2, base_sim);
					similaritySum = similaritySum + (bucket2.getConfidence() * similarity);
				}
				/*
				 * compute the similarity based on the confidence without similarity
				 */
				similaritySum = (similarityConstant * similaritySum) + bucket1.getConfidence();
				bucket1.setConfidenceWithSimilarity(similaritySum);
			}
			/*
			 * populate the confidence computed with similarity to be the exact new value for 
			 * the claim confidence
			 */
			for (ValueBucket bucket : bucketList) {
				bucket.setConfidence(bucket.getConfidenceWithSimilarity());
			}
		}
	}

	private void computeConfidence(double dampingFactor) {

		double e, denum;
		for (List<ValueBucket> bucketsList : kv.getDataItemsBuckets().values()) {
			for (ValueBucket b : bucketsList) {
				e = -1 * dampingFactor * b.getConfidence();
				e = Math.exp(e);
				denum = 1 + e;
				b.setConfidence(1/denum);
			}
		}
	}

	/**
	 * Compute the similarity between the two given claims.
	 * @param bucket1
	 * @param bucket2
	 * @return
	 */
	private double computeClaimsSimilarity(ValueBucket bucket1, ValueBucket bucket2, double base_sim) {
		double result = DataComparator.computeImplication(bucket1, bucket2, bucket1.getValueType());
		result = result - base_sim;
		if (Double.isNaN(result)) {
			return 1;
		}
		// TODO : revise for the non-string values  
		return (double)result;
	}

	/**
	 * Compute every source trustworthiness.
	 * The method doesn't delete the old trustworthiness value in order to be able to compute 
	 * the cosine similarity between the old and new trustworthiness values.
	 * it rather save it in the "oldTrustworthiness" property in the Source object.
	 */
	private void computeTrustworthiness(double dampingFactor) {
	
		double sum;
		for (Source source : kv.getSourcesHash().values()) {
			sum = 0;
			for (SourceClaim claim : source.getClaims()) {
				sum = sum + (1 - Math.exp((-1 * dampingFactor) * claim.getBucket().getConfidence()));
			}
			source.setOldTrustworthiness(source.getTrustworthiness());
			source.setTrustworthiness(sum / source.getClaims().size());
			//System.out.format("old trust:%f\t new trust:%f\n", source.getOldTrustworthiness(), source.getTrustworthiness()) ;
		}
	}

	
}
