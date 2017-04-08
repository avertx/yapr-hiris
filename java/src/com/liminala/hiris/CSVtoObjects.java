package com.liminala.hiris;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class CSVtoObjects {


	private static final Float MIN_VOLUME_TO_OUTPUT = 0.0f;

	static Comparator<TwoStockRecords> compPercentThenVolume = new Comparator<TwoStockRecords>(){

		@Override
		public int compare(TwoStockRecords o1, TwoStockRecords o2) {

			int pc = 0;
			pc = o1.getPercentChange().compareTo(o2.getPercentChange());

			if(pc != 0) return pc;

			return o1.getLater().getAdjVolume().compareTo(o2.getLater().getAdjVolume());
		}
	};
	static DateComparator compDate = new DateComparator();


	public static void main(String[] args) throws IOException, ParseException {

		LineNumberReader lnr = null;
//		FileWriter w = null;
		OutputStream os = null;
		ObjectOutputStream oos = null;
		try{
			File f = new File(args[0]);

			System.out.println("input file: "+f.getCanonicalPath())
			;
			File output = new File(args[1]);
			System.out.println("output file: "+output.getCanonicalPath());
			if(output.exists()){
				System.out.println("deleting existing output file: "+output.getCanonicalPath());
				output.delete();
			}
			System.out.println("creating output file: "+output.getCanonicalPath());
			
			output.createNewFile();
			os = new FileOutputStream(output);
			oos = new ObjectOutputStream(os);


			File obfMapFile = new File(args[2]);
			System.out.println("using obf map file file: "+obfMapFile.getCanonicalPath());
			if(!obfMapFile.exists()){
				throw new RuntimeException("obfmapfile doesn't exist: "+obfMapFile.getCanonicalPath());
			}
			HashMap<String, String> obfMap = getObfMap(obfMapFile);


			int lastFileLineNumber = 0;
			lnr = new LineNumberReader(new FileReader(f));

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			
			int count = 0;
			//			while(count < maxMatches){
			int errorCount = 0;

			HashMap<Ticker, TreeSet<StockRecord>> sorted = new HashMap<Ticker, TreeSet<StockRecord>>();
			
			HashSet<Ticker> tickersWithErrors = new HashSet<Ticker>();
			
			String line = null;
			while((line = lnr.readLine()) != null){
				//				if(line == null){
				//					System.out.println("null line reached, ending before maxMatches arg reached");
				//					break;
				//				}
				lastFileLineNumber = lnr.getLineNumber();
				//			System.out.printf("%d\n",lnr.getLineNumber());
				String[] s = line.split(",");


				Ticker ticker = new Ticker(s[0]);

				Date d = null;
				try {
					d = df.parse(s[1]);

//					Float unadjOpen = Float.parseFloat(s[2]);
//					Float unadjHigh = Float.parseFloat(s[3]);
//					Float unadjLow = Float.parseFloat(s[4]);
//					Float unadjClose = Float.parseFloat(s[5]);
//					Float unadjVolume = Float.parseFloat(s[6]);
//
					Float dividend = Float.parseFloat(s[7]);
//					Float splitRatio = Float.parseFloat(s[8]);
//
//
//					Float adjOpen = Float.parseFloat(s[9]);
//					Float adjHigh = Float.parseFloat(s[10]);
//					Float adjLow = Float.parseFloat(s[11]);
					Float adjClose = Float.parseFloat(s[12]);
					Float adjVolume = Float.parseFloat(s[13]);

					//StockRecord sr = new StockRecord(d, ticker, unadjOpen, unadjHigh, unadjLow, unadjClose, unadjVolume, dividend, splitRatio, adjOpen, adjHigh, adjLow, adjClose, adjVolume);
					StockRecord sr = new StockRecord(d, ticker);
					sr.setAdjClose(adjClose);
					sr.setAdjVolume(adjVolume);
					sr.setDividend(dividend);

					TreeSet<StockRecord> set = null;
					if((set = sorted.get(ticker)) == null){
						set = new TreeSet<StockRecord>(compDate);
						sorted.put(ticker, set);
					}
					set.add(sr);
					
					count++;
					
				}catch(RuntimeException e){
					errorCount++;
					System.out.printf("error on line %d, date %s, id: %s, total error count %d\n", count, s[1], ticker.getId(), errorCount);
					tickersWithErrors.add(ticker);
					continue;
				}
			}

			
			Set<Ticker> throwAwayTickers = sortOutInactives(sorted, df.parse("2016-01-08"));
			
			analyzeThrowAways(throwAwayTickers, tickersWithErrors);
			
			System.out.printf("starting to write sorted objects to file %s", output.getCanonicalPath());
			oos.writeObject(sorted);
			System.out.printf("ended writing objects to file %s", output.getCanonicalPath());
			
			for(Entry<Ticker, TreeSet<StockRecord>> entry : sorted.entrySet()){
				
				for(StockRecord sr : entry.getValue()){
					oos.writeObject(sr);
				}
			}
			
			
			int sumOfRecordCounts = 0;
			int minNumberOfRecords = Integer.MAX_VALUE;
			int maxNumberOfRecords = 0;
			Ticker maxRecordTicker = null;
			
			for(Entry<Ticker, TreeSet<StockRecord>> entry: sorted.entrySet()){
				System.out.printf("entry %s has %d records", entry.getKey().getId().toString(), entry.getValue().size());
				sumOfRecordCounts += entry.getValue().size();
				minNumberOfRecords = Math.min(entry.getValue().size(), minNumberOfRecords);
				
				if(entry.getValue().size() >= maxNumberOfRecords){
					maxNumberOfRecords = entry.getValue().size();
					maxRecordTicker = entry.getKey();
				}
			}
			
			System.out.printf("loaded and sorted %d tickers, avg count of records: %f, max count: %d, min count %d\n",sorted.size(), (float)sumOfRecordCounts/(float)sorted.size(), maxNumberOfRecords, minNumberOfRecords);
			

			System.out.printf("ended on file line number: %d, ticker count: %d,  %d errors\n", lastFileLineNumber, obfMap.size(), errorCount);




			
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}finally{
			lnr.close();
			oos.flush();
			oos.close();
			//			wObf.close();
		}
	}

	private static void analyzeThrowAways(Set<Ticker> throwAwayTickers, HashSet<Ticker> tickersWithErrors) {
		
		
		System.out.printf("%d tickers to throw away, %d with errors\n", throwAwayTickers.size(), tickersWithErrors.size() );
		throwAwayTickers.removeAll(tickersWithErrors);
		
		System.out.printf("after removing tickers with errors, number of throw away tickers remaining: %d\n",throwAwayTickers.size());
		
	}

	private static Set<Ticker> sortOutInactives(HashMap<Ticker, TreeSet<StockRecord>> sorted, Date d) {
		
		System.out.println("sorting out stocks with no records on "+d);
		
		Set<Ticker> throwAwayTickers = new HashSet<Ticker>();
		for(Entry<Ticker, TreeSet<StockRecord>> entry : sorted.entrySet()){
			if(entry.getValue().last().getDate().compareTo(d) != 0 ){
				throwAwayTickers.add(entry.getKey());
			}
		}
		
//		for(Ticker toRemove : throwAwayTickers){
//			sorted.remove(toRemove);
//		}

		System.out.printf("throwing away %d tickers\n", throwAwayTickers.size());
		sorted.keySet().removeAll(throwAwayTickers);

		return throwAwayTickers;
		
	}

	private static HashMap<String, String> getObfMap(File f) {

		HashMap<String, String> ret = new HashMap<String, String>();
		LineNumberReader lnr = null;
		try {

			lnr = new LineNumberReader(new FileReader(f));
			String line = null;
			while((line = lnr.readLine()) != null){
				String[] ids = line.split(",");

				String obf = ids[0].replaceAll("\"", "");
				String symbol = ids[1].replaceAll("\"", "");

				ret.put(symbol, obf);
			}

			System.out.printf("loaded %d obf symbols from file %s\n", ret.size(), f.getCanonicalPath());
			return ret;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}finally{
			try {
				lnr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}


	}



}
