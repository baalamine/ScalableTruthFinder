/**
 * 
 */
package org.qcri.cleaningdata.truthdiscovery.truthfinder.logical;


import java.util.ArrayList;
import java.util.HashMap;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.ml4all.logical.Update;

import qcri.dafna.dataModel.data.Source;
import qcri.dafna.dataModel.quality.dataQuality.ConvergenceTester;


/**
 * @author mlba
 * @created date: december 10, 2016
 * @desc: Truth Finder Update Logical Function
 */ 
public class TruthFinderUpdate extends Update<KeyValuePair<Integer, double[]>, KeyValuePair<Integer, qcri.dafna.dataModel.data.DataSet>> 
{

	/**
	 * 
	 */

	@SuppressWarnings("unchecked")
	@Override
	public KeyValuePair<Integer, double[]> update(KeyValuePair<Integer, qcri.dafna.dataModel.data.DataSet> input, RheemContext context) 
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
