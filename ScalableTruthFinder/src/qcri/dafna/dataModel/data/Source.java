package qcri.dafna.dataModel.data;
import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

/**
 * Source object
 * with set of claims: claim
 * @author dalia
 *
 */
public class Source implements Serializable {

	private String sourceIdentifier; 

	private double trustworthiness = 0.8; //change old value 0.9 to 0.8
	private double oldTrustworthiness = 0.8; //change old value 0.9 to 0.8
	private List<SourceClaim> claims;

	public Source(String sourceIdentifier) {
		this.sourceIdentifier = sourceIdentifier;
		claims = new ArrayList<SourceClaim>();
		oldTrustworthiness = trustworthiness;
	}
	public Source()
	{
	}

	public void setTrustworthiness(double trustworthiness) {
		this.trustworthiness = trustworthiness;
	}
	public double getTrustworthiness() {
		return trustworthiness;
	}
	public void setOldTrustworthiness(double oldTrustworthiness) {
		this.oldTrustworthiness = oldTrustworthiness;
	}
	public double getOldTrustworthiness() {
		return oldTrustworthiness;
	}
	public List<SourceClaim> getClaims() {
		return claims;
	}
	public void addClaim(SourceClaim claim) {
		claims.add(claim);
	}
	public String getSourceIdentifier() {
		return sourceIdentifier;
	}

	void resetSourceTrustworthiness(double value) {
		trustworthiness = value;
		oldTrustworthiness = value;
	}

}
