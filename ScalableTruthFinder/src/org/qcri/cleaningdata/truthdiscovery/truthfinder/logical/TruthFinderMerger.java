/**
 * 
 */
package org.qcri.cleaningdata.truthdiscovery.truthfinder.logical;

import java.util.ArrayList;

import org.qcri.rheem.core.logicallayer.GlobalVariableMerger;

import gnu.trove.map.hash.THashMap;

/**
 * @author mlba
 * @created date: Feb 1, 2016
 * Truth Finder Merger
 */
public class TruthFinderMerger extends GlobalVariableMerger {

	@Override
	public THashMap<String, Object> merge(THashMap<String, Object> hashMap) 
	{
		// TODO Auto-generated method stub
		//System.out.println("Merger Op") ;
		THashMap<String, Object> newHashMap = new THashMap<>();
        hashMap.forEach((k,v) -> {
        	//System.out.println(v.getClass().getName());
        	//System.out.println(k+":::"+v) ;
            newHashMap.put(k, v);
            });
        
        return newHashMap;
       
	}

}
