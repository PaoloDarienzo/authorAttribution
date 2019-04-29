package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StatsWritable implements WritableComparable<StatsWritable> {
	
	private Text onAuthor;
	
	private FloatWritable avgWordLengthRatio;
	
	private FloatWritable functionDensityRatio;
	private FloatWritable punctuationDensityRatio;
	
	private FloatWritable typeTokenRatio; //vocabulary richness
	
	public String commenti = "";
	
	public StatsWritable() {
		
		onAuthor = new Text();
		
		avgWordLengthRatio = new FloatWritable(0);
		
		functionDensityRatio = new FloatWritable(0);
		punctuationDensityRatio = new FloatWritable(0);
				
		typeTokenRatio = new FloatWritable(0);
		
	}
	
	public StatsWritable(AuthorTrace authorTrace, AuthorTrace authorTraceUnk) {
		
		setOnAuthor(authorTrace.getAuthor());

		setAvgWordLengthRatio(
				MethodsCollection.getFloatRatio(
						authorTraceUnk.getAvgWordLength(), authorTrace.getAvgWordLength()
						)
				);
		
		//AuthorTrace.functionDensity has a value in range 0-1;
		float unkFuncValue = authorTraceUnk.getFunctionDensity().get();
		float knownFuncValue = authorTrace.getFunctionDensity().get();
		setFunctionDensityRatio(
				MethodsCollection.getFloatRatio(unkFuncValue, knownFuncValue)
				);
		
		//AuthorTrace.punctuationDensity has a value in range 0-1;
		float unkPunctValue = authorTraceUnk.getPunctuationDensity().get();
		float knownPunctValue = authorTrace.getPunctuationDensity().get();
		setPunctuationDensityRatio(
				MethodsCollection.getFloatRatio(unkPunctValue, knownPunctValue)
				);
			
		//TTR: num different words / num total words
		float authUnkTTR = authorTraceUnk.getTTR().get();
		float authKnownTTR = authorTrace.getTTR().get();
		setTypeTokenRatio(
				MethodsCollection.getFloatRatio(authUnkTTR, authKnownTTR)
				);
		
	}

	public Text getOnAuthor() {
		return onAuthor;
	}

	public void setOnAuthor(Text onAuthor) {
		this.onAuthor = onAuthor;
	}
	
	public void setOnAuthor(String onAuthor) {
		this.onAuthor = new Text(onAuthor);
	}

	public FloatWritable getAvgWordLengthRatio() {
		return avgWordLengthRatio;
	}

	public void setAvgWordLengthRatio(FloatWritable avgWordLengthRatio) {
		this.avgWordLengthRatio = avgWordLengthRatio;
	}
	
	public void setAvgWordLengthRatio(float avgWordLengthRatio) {
		this.avgWordLengthRatio = new FloatWritable(avgWordLengthRatio);
	}

	public FloatWritable getFunctionDensityRatio() {
		return functionDensityRatio;
	}

	public void setFunctionDensityRatio(FloatWritable functionDensityRatio) {
		this.functionDensityRatio = functionDensityRatio;
	}
	
	public void setFunctionDensityRatio(float functionDensityRatio) {
		this.functionDensityRatio = new FloatWritable(functionDensityRatio);
	}

	public FloatWritable getPunctuationDensityRatio() {
		return punctuationDensityRatio;
	}

	public void setPunctuationDensityRatio(FloatWritable punctuationDensityRatio) {
		this.punctuationDensityRatio = punctuationDensityRatio;
	}

	public void setPunctuationDensityRatio(float punctuationDensityRatio) {
		this.punctuationDensityRatio = new FloatWritable(punctuationDensityRatio);
	}
	
	public void setTypeTokenRatio(FloatWritable typeTokenRatio) {
		this.typeTokenRatio = 	typeTokenRatio;
	}
	
	public void setTypeTokenRatio(float typeTokenRatio) {
		this.typeTokenRatio = new FloatWritable(typeTokenRatio);
	}
	
	public FloatWritable getTypeTokenRatio() {
		return this.typeTokenRatio;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.onAuthor.readFields(in);
		
		this.avgWordLengthRatio.readFields(in);
		this.functionDensityRatio.readFields(in);
		this.punctuationDensityRatio.readFields(in);
		
		this.typeTokenRatio.readFields(in);
		
		//commenti
		int sizeString = in.readInt();
		byte[] bytes = new byte[sizeString];
		for(int i = 0; i < sizeString; i++) {
			bytes[i] = in.readByte();
		}
		commenti = new String(bytes);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		onAuthor.write(out);
		
		avgWordLengthRatio.write(out);
		functionDensityRatio.write(out);
		punctuationDensityRatio.write(out);
		
		typeTokenRatio.write(out);
		
		//commenti
		out.writeInt(commenti.length());
    	out.writeBytes(commenti);
		
	}

	@Override
	public int compareTo(StatsWritable at) {
		
		//StatsWritable gets 2 arguments: 
		//the known profile, the unknown profile.
		//there is only 1 profile for known author, so
		//it should suffice doing the compare on the name of the author
		//which the unknown profile is compared to.
		
		return onAuthor.compareTo(at.onAuthor);
		
		/*
		int cmp = onAuthor.compareTo(at.onAuthor);
		if (cmp != 0)
			return cmp;
		cmp = avgWordLengthRatio.compareTo(at.avgWordLengthRatio);
		if (cmp != 0)
			return cmp;
		cmp = functionDensityRatio.compareTo(functionDensityRatio);
		if (cmp != 0)
			return cmp;
		cmp = punctuationDensityRatio.compareTo(at.punctuationDensityRatio);
		if (cmp != 0)
			return cmp;
		cmp = wordCountSizeRatio.compareTo(at.wordCountSizeRatio);
		if (cmp != 0)
			return cmp;
		cmp = twoGramsSizeRatio.compareTo(at.twoGramsSizeRatio);
		if (cmp != 0)
			return cmp;
		return threeGramsSizeRatio.compareTo(at.threeGramsSizeRatio);
		*/
		
	}
	
	@Override
	public String toString() {
		
		//String values of float with 6 values
		String avgWordLenToString = String.format("%6f", avgWordLengthRatio.get());
		String funcDensToString = String.format("%6f", functionDensityRatio.get());
		String punctDensToString = String.format("%6f", punctuationDensityRatio.get());
		String typeTokenRatioToString = String.format("%6f", typeTokenRatio.get());
		
		String statsToString = "Author: " + onAuthor.toString() + "\n";
		statsToString += "Average word length ratio: " + avgWordLenToString + "\n";
		statsToString += "function density ratio: " + funcDensToString + "\n";
		statsToString += "punctuation density ratio: " + punctDensToString + "\n";
		statsToString += "TTR: " + typeTokenRatioToString + "\n";
		statsToString += "Commenti: " + this.commenti + "\n";
		return statsToString;
		
	}

}
