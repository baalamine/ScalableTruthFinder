/**
 * 
 */
package org.qcri.cleaningdata.truthdiscovery.truthfinder.logical;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.ml4all.logical.Transform;

import qcri.dafna.dataModel.data.Source;
import qcri.dafna.dataModel.data.SourceClaim;


/**
 * @author mlba
 * @created date: december 10, 2016
 * @desc: Truth Finder Transform Logical Functon
 */
public class TruthFinderTransform extends Transform<KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>>, String>
{
	
	@Override
	public KeyValuePair<Integer, KeyValuePair<Integer, List<SourceClaim>>> transform(String line, RheemContext context) {
		
		// TODO Auto-generated method stub
		//System.out.println("Transform Op") ;
		String regexp	    = "\",\"" ;
		Pattern p 			= Pattern.compile(regexp) ;
		String[] record 	= p.split(line, 7);
		
		int claimId 		= Integer.parseInt(record[0].replace("\"", "")) ;
		String objectId 	= record[1].replace("\"", "") ;
		String propertyName = record[2].replace("\"", "") ; 
		String stringValue 	= record[3].replace("\"", "") ; 
		String sourceId 	= record[4].replace("\"", "") ; 
		String timeStamp 	= record[5].replace("\"", "") ; 
		
		
		// when the dataset is clean this should be removed
		if (stringValue.equals("Not Available") || stringValue.trim().isEmpty()) {
			return new KeyValuePair<> (1, new KeyValuePair<>(1, null)) ;
		}
		if (objectId == null || objectId.trim().equalsIgnoreCase("null")) {
			return new KeyValuePair<> (1, new KeyValuePair<>(1, null)) ;
		}
		
		SourceClaim sc = new SourceClaim (claimId, objectId, "", propertyName, stringValue, 1.0, timeStamp, new Source(sourceId) ) ;
		List<SourceClaim> r = new ArrayList<SourceClaim> () ;
		r.add(sc) ;
	
		
		return new KeyValuePair<>(1, new KeyValuePair<>(1, r)) ;
	}
	
	
}
