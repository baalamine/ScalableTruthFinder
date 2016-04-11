/**
 * 
 */
package org.qcri.truthdiscovery.truthfinder.logical;

import java.util.ArrayList;

import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.truthdiscovery.logical.Loop;


/**
 * @author mlba
 * @created date: december 10, 2016
 * @desc: Truth Finder Loop Function
 */
public class TruthFinderLoop extends Loop<ArrayList<KeyValuePair<Integer, double[]>>>
{
	/**
	 * 
	 */
	double cosineThresold ;

	public TruthFinderLoop(double cosineThresold) {
		// TODO Auto-generated constructor stub
		this.cosineThresold = cosineThresold ;	
	}

	@Override
	public boolean condition(ArrayList<KeyValuePair<Integer, double[]>> input, RheemContext context) {
		// TODO Auto-generated method stub
		System.out.println("LoopOp") ;
	
		double[] cosines = input.get(0).value ;
		System.out.println("[" + cosines[0] + "," + cosines[1] + "]") ;
		
		if ( 1 - cosines[0] > this.cosineThresold )
		{
			return true ;
		}
		
		return false ;
		
	}
	

}
