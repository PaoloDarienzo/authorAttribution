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
	
	private FloatWritable wordCountSizeRatio;	
	private FloatWritable twoGramsSizeRatio;
	private FloatWritable threeGramsSizeRatio;
	
	public StatsWritable() {
		onAuthor = new Text();
		
		avgWordLengthRatio = new FloatWritable(0);
		functionDensityRatio = new FloatWritable(0);
		punctuationDensityRatio = new FloatWritable(0);
		
		wordCountSizeRatio = new FloatWritable(0);	
		twoGramsSizeRatio = new FloatWritable(0);
		threeGramsSizeRatio = new FloatWritable(0);
		
	}
	
	public StatsWritable(AuthorTrace authorTrace, AuthorTrace authorTraceUnk) {
		
		setOnAuthor(authorTrace.getAuthor());

		setAvgWordLengthRatio(
				MethodsCollection.getFloatRatio(
						authorTraceUnk.getAvgWordLength(), authorTrace.getAvgWordLength()
						)
				);
		
		setFunctionDensityRatio(
				MethodsCollection.getFloatRatio(
						authorTraceUnk.getFunctionDensity(), authorTrace.getFunctionDensity()
						)
				);
		
		setPunctuationDensityRatio(
				MethodsCollection.getFloatRatio(
						authorTraceUnk.getPunctuationDensity(), authorTrace.getPunctuationDensity()
						)
				);
		
		///
		setWordCountSizeRatio(
				MethodsCollection.getSizeRatio(
						authorTraceUnk.getWordsArray().getArray().size(),
						authorTrace.getWordsArray().getArray().size()
						)
				);
		
		setTwoGramsSizeRatio(
				MethodsCollection.getSizeRatio(
						authorTraceUnk.getFinalTwoGrams().getTwoGrams().size(), 
						authorTrace.getFinalTwoGrams().getTwoGrams().size()
						)
				);
		
		setThreeGramsSizeRatio(
				MethodsCollection.getSizeRatio(
						authorTraceUnk.getFinalThreeGrams().getThreeGrams().size(), 
						authorTrace.getFinalThreeGrams().getThreeGrams().size()
						)
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
	
	public FloatWritable getWordCountSizeRatio() {
		return wordCountSizeRatio;
	}

	public void setWordCountSizeRatio(FloatWritable wordCountSizeRatio) {
		this.wordCountSizeRatio = wordCountSizeRatio;
	}
	
	public void setWordCountSizeRatio(float wordCountSizeRatio) {
		this.wordCountSizeRatio = new FloatWritable(wordCountSizeRatio);
	}

	public FloatWritable getTwoGramsSizeRatio() {
		return twoGramsSizeRatio;
	}

	public void setTwoGramsSizeRatio(FloatWritable twoGramsSizeRatio) {
		this.twoGramsSizeRatio = twoGramsSizeRatio;
	}
	
	public void setTwoGramsSizeRatio(float twoGramsSizeRatio) {
		this.twoGramsSizeRatio = new FloatWritable(twoGramsSizeRatio);
	}

	public FloatWritable getThreeGramsSizeRatio() {
		return threeGramsSizeRatio;
	}

	public void setThreeGramsSizeRatio(FloatWritable threeGramsSizeRatio) {
		this.threeGramsSizeRatio = threeGramsSizeRatio;
	}
	
	public void setThreeGramsSizeRatio(float threeGramsSizeRatio) {
		this.threeGramsSizeRatio = new FloatWritable(threeGramsSizeRatio);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.onAuthor.readFields(in);
		this.avgWordLengthRatio.readFields(in);
		this.functionDensityRatio.readFields(in);
		this.punctuationDensityRatio.readFields(in);
		this.wordCountSizeRatio.readFields(in);
		this.twoGramsSizeRatio.readFields(in);
		this.threeGramsSizeRatio.readFields(in);
				
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		onAuthor.write(out);
		avgWordLengthRatio.write(out);
		functionDensityRatio.write(out);
		punctuationDensityRatio.write(out);
		wordCountSizeRatio.write(out);
		twoGramsSizeRatio.write(out);
		threeGramsSizeRatio.write(out);
		
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
		
		String statsToString = "Author: " + onAuthor.toString() + "\n";
		statsToString += "Average word length ratio: " + avgWordLengthRatio.toString() + "\n";
		statsToString += "function density ratio: " + functionDensityRatio.toString() + "\n";
		statsToString += "punctuation density ratio: " + punctuationDensityRatio.toString() + "\n";
		statsToString += "word count size ratio: " + wordCountSizeRatio.toString() + "\n";
		statsToString += "twoGrams size ratio: " + twoGramsSizeRatio.toString() + "\n";
		statsToString += "threeGrams size ratio: " + threeGramsSizeRatio.toString() + "\n";
		return statsToString;
		
	}

}
