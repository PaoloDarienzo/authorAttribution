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
		
		float avgWordLengthUnk = authorTraceUnk.getAvgWordLength().get();
		float avgWordLengthKnown = authorTrace.getAvgWordLength().get();
		setAvgWordLengthRatio(new FloatWritable((float) avgWordLengthUnk / avgWordLengthKnown));
		
		float funcUnk = authorTraceUnk.getFunctionDensity().get();
		float funcKnown = authorTrace.getFunctionDensity().get();
		setFunctionDensityRatio(new FloatWritable((float) funcUnk / funcKnown));
		
		float punctUnk = authorTraceUnk.getPunctuationDensity().get();
		float punctKnown = authorTrace.getPunctuationDensity().get();
		setPunctuationDensityRatio(new FloatWritable((float) punctUnk / punctKnown));
		
		float wordCountUnkSize = authorTraceUnk.getWordsArray().getArray().size();
		float wordCountKnownSize = authorTrace.getWordsArray().getArray().size();
		setWordCountSizeRatio(new FloatWritable((float) wordCountUnkSize / wordCountKnownSize));
		
		float twoGramsUnkSize = authorTraceUnk.getFinalTwoGrams().getTwoGrams().size();
		float twoGramsKnownSize = authorTrace.getFinalTwoGrams().getTwoGrams().size();
		setTwoGramsSizeRatio(new FloatWritable((float) twoGramsUnkSize / twoGramsKnownSize));
		
		float threeGramsUnkSize = authorTraceUnk.getFinalThreeGrams().getThreeGrams().size();
		float threeGramsKnownSize = authorTrace.getFinalThreeGrams().getThreeGrams().size();
		setThreeGramsSizeRatio(new FloatWritable((float) threeGramsUnkSize / threeGramsKnownSize));
		
	}

	public Text getOnAuthor() {
		return onAuthor;
	}

	public void setOnAuthor(Text onAuthor) {
		this.onAuthor = onAuthor;
	}

	public FloatWritable getAvgWordLengthRatio() {
		return avgWordLengthRatio;
	}

	public void setAvgWordLengthRatio(FloatWritable avgWordLengthRatio) {
		this.avgWordLengthRatio = avgWordLengthRatio;
	}

	public FloatWritable getFunctionDensityRatio() {
		return functionDensityRatio;
	}

	public void setFunctionDensityRatio(FloatWritable functionDensityRatio) {
		this.functionDensityRatio = functionDensityRatio;
	}

	public FloatWritable getPunctuationDensityRatio() {
		return punctuationDensityRatio;
	}

	public void setPunctuationDensityRatio(FloatWritable punctuationDensityRatio) {
		this.punctuationDensityRatio = punctuationDensityRatio;
	}

	public FloatWritable getWordCountSizeRatio() {
		return wordCountSizeRatio;
	}

	public void setWordCountSizeRatio(FloatWritable wordCountSizeRatio) {
		this.wordCountSizeRatio = wordCountSizeRatio;
	}

	public FloatWritable getTwoGramsSizeRatio() {
		return twoGramsSizeRatio;
	}

	public void setTwoGramsSizeRatio(FloatWritable twoGramsSizeRatio) {
		this.twoGramsSizeRatio = twoGramsSizeRatio;
	}

	public FloatWritable getThreeGramsSizeRatio() {
		return threeGramsSizeRatio;
	}

	public void setThreeGramsSizeRatio(FloatWritable threeGramsSizeRatio) {
		this.threeGramsSizeRatio = threeGramsSizeRatio;
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
