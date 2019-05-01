package support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StatsWritable implements WritableComparable<StatsWritable> {
	
	private Text onAuthor;
	
	private FloatWritable avgWordLengthRatio;
	
	private FloatWritable functionDensityRatio;
	private FloatWritable punctuationDensityRatio;
	
	private FloatWritable typeTokenRatio; //vocabulary richness
	
	private FloatWritable wordFreqRatio;
	
	private FloatWritable twoGramsRatio;
	private FloatWritable threeGramsRatio;
	
	public String commenti = "";
	
	public StatsWritable() {
		
		onAuthor = new Text();
		
		avgWordLengthRatio = new FloatWritable(0);
		
		functionDensityRatio = new FloatWritable(0);
		punctuationDensityRatio = new FloatWritable(0);
				
		typeTokenRatio = new FloatWritable(0);
		
		wordFreqRatio = new FloatWritable(0);
		
		twoGramsRatio = new FloatWritable(0);
		threeGramsRatio = new FloatWritable(0);
		
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
		
		//wordFreqRatio
		HashMap<String, Float> knownWordFreq = authorTrace.getWordsFreqArray().getArray();
		HashMap<String, Float> unkWordFreq = authorTraceUnk.getWordsFreqArray().getArray();
		setWordFreqRatioRatio(MethodsCollection.getWordFreqRatioFromAll(knownWordFreq, unkWordFreq));
		
		//twoGramsRatio
		HashMap<TextPair, Integer> unkTwoGrams = authorTraceUnk.getFinalTwoGrams().getTwoGrams();
		HashMap<TextPair, Integer> knownTwoGrams = authorTrace.getFinalTwoGrams().getTwoGrams();
		setTwoGramsRatio(MethodsCollection.getTwoGramsRatio(knownTwoGrams, unkTwoGrams));
		
		//threeGramsRatio
		HashMap<TextTrigram, Integer> unkThreeGrams = authorTraceUnk.getFinalThreeGrams().getThreeGrams();
		HashMap<TextTrigram, Integer> knownThreeGrams = authorTrace.getFinalThreeGrams().getThreeGrams();
		setThreeGramsRatio(MethodsCollection.getThreeGramsRatio(knownThreeGrams, unkThreeGrams));
		
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
		this.typeTokenRatio = typeTokenRatio;
	}
	
	public void setTypeTokenRatio(float typeTokenRatio) {
		this.typeTokenRatio = new FloatWritable(typeTokenRatio);
	}
	
	public FloatWritable getTypeTokenRatio() {
		return this.typeTokenRatio;
	}
	
	public void setWordFreqRatioRatio(FloatWritable wordFreqRatio) {
		this.wordFreqRatio = wordFreqRatio;
	}
	
	public void setWordFreqRatioRatio(float wordFreqRatio) {
		this.wordFreqRatio = new FloatWritable(wordFreqRatio);
	}
	
	public FloatWritable getWordFreqRatioRatio() {
		return this.wordFreqRatio;
	}
	
	public void setTwoGramsRatio(FloatWritable twoGramsRatio) {
		this.twoGramsRatio = twoGramsRatio;
	}
	
	public void setTwoGramsRatio(float twoGramsRatio) {
		this.twoGramsRatio = new FloatWritable(twoGramsRatio);
	}
	
	public FloatWritable getTwoGramsRatio() {
		return this.twoGramsRatio;
	}
	
	public void setThreeGramsRatio(FloatWritable threeGramsRatio) {
		this.threeGramsRatio = threeGramsRatio;
	}
	
	public void setThreeGramsRatio(float threeGramsRatio) {
		this.threeGramsRatio = new FloatWritable(threeGramsRatio);
	}
	
	public FloatWritable getThreeGramsRatio() {
		return this.threeGramsRatio;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.onAuthor.readFields(in);
		
		this.avgWordLengthRatio.readFields(in);
		this.functionDensityRatio.readFields(in);
		this.punctuationDensityRatio.readFields(in);
		
		this.typeTokenRatio.readFields(in);
		
		this.wordFreqRatio.readFields(in);
		
		this.twoGramsRatio.readFields(in);
		this.threeGramsRatio.readFields(in);
		
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
		
		wordFreqRatio.write(out);
		
		twoGramsRatio.write(out);
		threeGramsRatio.write(out);
		
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
		
	}
	
	@Override
	public String toString() {
		
		//String values of float with 6 values
		String avgWordLenToString = String.format("%6f", avgWordLengthRatio.get());
		String funcDensToString = String.format("%6f", functionDensityRatio.get());
		String punctDensToString = String.format("%6f", punctuationDensityRatio.get());
		String typeTokenRatioToString = String.format("%6f", typeTokenRatio.get());
		String wordFreqRatioToString = String.format("%6f", wordFreqRatio.get());
		String twoGramsRatioToString = String.format("%6f", twoGramsRatio.get());
		String threeGramsRatioToString = String.format("%6f", threeGramsRatio.get());
		
		String statsToString = "Author: " + onAuthor.toString() + "\n";
		statsToString += "Average word length ratio: " + avgWordLenToString + "\n";
		statsToString += "Function density ratio: " + funcDensToString + "\n";
		statsToString += "Punctuation density ratio: " + punctDensToString + "\n";
		statsToString += "TTR: " + typeTokenRatioToString + "\n";
		statsToString += "Word frequencies ratio (20-60): " + wordFreqRatioToString + "\n";
		statsToString += "TwoGrams ratio: " + twoGramsRatioToString + "\n";
		statsToString += "ThreeGrams ratio: " + threeGramsRatioToString + "\n";
		statsToString += "Commenti: " + this.commenti + "\n";
		
		return statsToString;
		
	}

}
