/**
 * 
 */
package org.qcri.truthdiscovery.truthfinder.logical;


import java.util.ArrayList;
import java.util.HashMap;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Update;
import org.qcri.truthdiscovery.util.dataModel.data.Source;
import org.qcri.truthdiscovery.util.dataModel.quality.dataQuality.ConvergenceTester;


/**
 * @author mlba
 * @created date: december 10, 2016
 * @desc: Truth Finder Update Logical Function
 */ 
public class TruthFinderUpdate extends Update<KeyValuePair<Integer, double[]>, KeyValuePair<Integer, org.qcri.truthdiscovery.util.dataModel.data.DataSet>> 
{

	/**
	 * 
	 */

	@SuppressWarnings("unchecked")
	@Override
	public KeyValuePair<Integer, double[]> update(KeyValuePair<Integer, org.qcri.truthdiscovery.util.dataModel.data.DataSet> input, RheemContext context) 
	{
		// TODO Auto-generated method stub
		System.out.println("Update Operator") ;
		double[] cosineSimilarities = new double[2] ;
		
		ArrayList<String> trustworthinessScores  = new ArrayList<String> () ;
		
		//update trustworthiness scores and push 
		for (Source s : input.value.getSourcesHash().values())
			trustworthinessScores.add( s.getSourceIdentifier() + "===>" + s.getTrustworthiness() ) ; 
		context.push("trustworthiness", trustworthinessScores) ;
		
		//push params again
		ArrayList<Double> params = (ArrayList<Double>) context.getByKey("params") ;
		context.push("params", params) ;
		
			
	
		double newCosineSimilarity = ConvergenceTester.computeTrustworthinessCosineSimilarity(input.value);
		double newConfCosineSimilarity = ConvergenceTester.computeConfidenceCosineSimilarity(input.value);
		
		cosineSimilarities[0] = newCosineSimilarity ;
		cosineSimilarities[1] = newConfCosineSimilarity ;
	
		return new KeyValuePair<Integer, double[]>(1, cosineSimilarities);
	}
	
	
}
