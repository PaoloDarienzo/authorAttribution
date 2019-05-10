# authorAttribution
MapReduce project, UniVr, 2018/2019.

Obiettivo:
analizzare libri (da project Gutenberg) e "profilare" ogni autore in base a:

- parole utilizzate (frequenza, lunghezza media)
- densità di punteggiatura
- densità di utilizzo di parole dette "function words"
- accostamenti di parole utilizzate (vicinanza; 2grams e 3grams)


-> PROFILAZIONE AUTORE

	INPUT:

	Tutti i file.txt;
	Ogni file ha come nome:
	-> author,___,title.txt

	MAP:
		Per ogni file, raccoglie le informazioni volute.
		Estrapola il nome dell'autore dal nome del file stesso.
		
	PARTITIONER:
		Per ogni file, viene estratto il campo del nome dell'autore.
		In base al nome dell'autore, verrà selezionato un reducer.
		n = numero autori; r = numero reducer.
		r >= n ---> ogni reducer si occupa di un autore; le informazioni relative
					a quell'autore (nel calcolo percentuale) è preservata.
		r < n  ---> ERRORE; se i reducer indicati non sono sufficienti,
					verranno indicati stessi reducer per autori diversi 
					(rispettivamente al modulo; se ci sono 6 autori e vengono indicati 4 reducer,
					6 % 4 = 2, i primi due reducer ricevono ognuno 2 autori diversi);
					pertanto i risultati saranno falsati.

	REDUCE:
		Per ogni file, somma i valori ricevuti dai libri analizzati.
		Genererà così una serie di valori che saranno l'impronta dell'autore.
		
	OUTPUT:
	
	Un file per ogni autore.
	Ogni file ha come nome:
	-> nome_autore-r-dddddd
	
-> DEDUZIONE AUTORE

	INPUT
	
	Uno o più file di testo di cui l'autore non è conosciuto.
	
	MAP:
		Creazione dell'impronta del singolo file;
		Estrazione dei valori desiderati.
	
	REDUCER:
		Per ogni file, calcolo valori finali.
		
	OUTPUT
		Un file per ogni impronta generata sconosciuta.
	
	MAP:
		Load dei file sconosciuti e di un profilo noto.
		Generazione statistiche di confronto tra ogni file sconosciuto e il profilo noto.
	REDUCE :
		Riceve tutti i risultati dei confronti ed emette una lista ordinata
		di somiglianza.
		
	OUTPUT
		Un file per ogni autore sconosciuto, con una lista ordinata per punteggio di somiglianza;
		Per ogni autore, il punteggio di somiglianza.

Struttura repository:
La directory "script" contiene tutti gli script utilizzati nella gestione della base di dati.
Il file "Struttura_progetto.txt" contiene le scelte progettuali e di gestione del database.
Il file "count.txt" contiene la lista di ogni autore e del numero di opere per ogni autore.
