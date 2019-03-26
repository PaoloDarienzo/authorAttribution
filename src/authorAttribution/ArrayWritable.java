package authorAttribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class ArrayWritable implements Writable {
	private HashMap<String, Integer> hm;

	public ArrayWritable() {
		this.hm = new HashMap<String, Integer>();
	}
	
	public HashMap<String, Integer> getArray() {
		return this.hm;
	}
	
	public void setArray(HashMap<String, Integer> hm) {
		this.hm = hm;
	}
	
	public void clear() {
		hm.clear();
	}

	public String toString() {
		return hm.toString();
	}
		
	/*
	 * stringa di 4 char: 5 elem: 4 char + char di chiusura stringa
	 * 
	 * Una stripe e' un dizionario, l'hashmap in questa versione
	 * l'hashmap come viene serializzato:
	 * un dizionario e' un insieme di coppie chiave, valore
	 * abbiamo n coppie
	 * ogni coppia e' di due parti, una string e un integer;
	 * integer e' un num di byte fisso
	 * quindi: N, numk1, k1, v1, numk2, k2, v2, ..., numkn, kn, vn
	 * ->conto quanti oggetti ha dizionario
	 * out.write(int) (N)
	 * ciclo:
	 *  per ogni k-v prendo k.length; out.write(int); out.writebytes(k), out.write(int)(per val)....
	 *  
	 *  Deserializzazione e' procedura inversa:
	 *  lo stream e' il puntatore al primo byte della struttura dati serializzata
	 *  read.int() e leggo N
	 *  for(n volte)
	 *  read int che legge i caratteri da assegnare alla chiave;
	 *  leggo i caratteri;
	 *  read int per leggere il valore;
	 *  ho popolato il primo valore;
	 *  per N volte.
	 */
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(this.hm.size());
		
		for(Entry<String, Integer> entry : hm.entrySet()) {
			String key = entry.getKey();
        	out.writeInt(key.length());
        	out.writeBytes(key);   
        	out.writeInt(entry.getValue());
        }
		
		//altra versione
		/*
		Iterator<Entry<String, Integer>> it = hm.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String, Integer> pairs = it.next();
			String k = pairs.getKey();
			Integer v = pairs.getValue();
			out.writeInt(k.length());
			out.writeBytes(k);
			out.writeInt(v);
		}
		*/
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		hm.clear();
		
		int size = in.readInt();
		
		for(int i = 0; i < size; i++) {
			
			int sizeKey = in.readInt();
			byte[] bytes = new byte[sizeKey];
			for(int j = 0; j < sizeKey; j++) {
				bytes[j] = in.readByte();
			}
			
			String key = new String(bytes);
			
			Integer value = in.readInt();
			
			this.hm.put(key, value);
			
		}
		
	}
	
	public void sum(ArrayWritable h) {
		
		HashMap<String, Integer> ht = h.getArray();
		
		for(Entry<String, Integer> entry : ht.entrySet()) {
			String key = entry.getKey();
			if(this.hm.containsKey(key)) {
				this.hm.put(key, this.hm.get(key) + entry.getValue());
			}
			else {
				this.hm.put(key, entry.getValue());
			}
        }
		
		/*
		 * io sono this.hm, mi viene passato un h
		 * per ogni chiave k di h, lo sommo alla chiave k di this.hm, se esiste
		 * se non esiste lo creo a 1
		 * 
		 * h deve essere iterabile, creo iteratore su entrySet di h (chiamato it);
		 * finche' it ha elemento, per ogni elemento (di tipo <k, v>
		 * prendo chiave e valore e li sommo al mio array this.hm (tramite metodo increment)
		 * 
		 */
		
		//Versione prof
		/*
		Iterator<Entry<String, Integer>> it = h.entrySet().iterator();
		while(it.hasNext()) {
			//avendo chiamato la next, non posso utilizzare in this.increment
			//it.next().getKey() in quanto, quando faro'
			//it.next().getValue() sara' il valore dell'elemento di it successivo
			//E non il value relativo alla key
			Map.Entry<String, Integer> pairs = it.next();
			this.increment(pairs.getKey(), pairs.getValue());
		}
		*/
		
	}
	
	public boolean containsKey(String otherWord) {
		return hm.containsKey(otherWord);
	}
	
	//I metodi seguenti sono utilizzati nella versione del professore
	public void increment(String t, int value) {
		int count = value;
		if(hm.containsKey(t)) {
			count += hm.get(t) + count;
		}
		hm.put(t, count);
	}
	
	public void increment(String t) {
		this.increment(t, 1);
	}
	
	public Set<Entry<String, Integer>> entrySet(){
		return hm.entrySet();
	}

}