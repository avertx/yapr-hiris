package com.liminala.hiris;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

public class CSVFilter {


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
	static Comparator<StockRecord> compDate = new Comparator<StockRecord>(){

		@Override
		public int compare(StockRecord o1, StockRecord o2) {

			return o1.getDate().compareTo(o2.getDate());
		}

	};


	private static ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat>(){

		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd");
		}
	};
	
	
	public static void main(String[] args) throws IOException, ParseException {

		LineNumberReader lnr = null;
		FileWriter w = null;
		
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
			w = new FileWriter(output);



			File obfMapFile = new File(args[2]);
			System.out.println("using obf map file file: "+obfMapFile.getCanonicalPath());
			if(!obfMapFile.exists()){
				throw new RuntimeException("obfmapfile doesn't exist: "+obfMapFile.getCanonicalPath());
			}
			HashMap<String, String> obfMap = getObfMap(obfMapFile);
			for(Entry<String, String> e : obfMap.entrySet()){
				Ticker.getUuidForTicker().put(e.getKey(), UUID.fromString(e.getValue()));
			}
			for(Entry<String, String> e : obfMap.entrySet()){
				Ticker.getTickerForUUID().put(UUID.fromString(e.getValue()), e.getKey());
			}

			int lastFileLineNumber = 0;
			lnr = new LineNumberReader(new FileReader(f));

			//DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			
			
			//			while(count < maxMatches){
			int errorCount = 0;

			HashMap<Ticker, TreeSet<StockRecord>> sorted = new HashMap<Ticker, TreeSet<StockRecord>>();
			
			HashSet<Ticker> tickersWithErrors = new HashSet<Ticker>();
			
//			String mustContainYear = "1989";
//			String mustContainDateForTicker = "1989-12-29";
			
			AtomicLong parseCount = new AtomicLong();
			
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
					d = df.get().parse(s[1]);
					String year = s[1].split("-")[0];
//					if(!year.equals(mustContainYear)) continue;
					
//					Float unadjOpen = Float.parseFloat(s[2]);
//					Float unadjHigh = Float.parseFloat(s[3]);
//					Float unadjLow = Float.parseFloat(s[4]);
//					Float unadjClose = Float.parseFloat(s[5]);
//					Float unadjVolume = Float.parseFloat(s[6]);
//
					Float dividend = Float.parseFloat(s[7]);
					Float splitRatio = Float.parseFloat(s[8]);
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
					sr.setSplitRatio(splitRatio);
					
					sorted.computeIfAbsent(ticker, h -> new TreeSet<StockRecord>(compDate)).add(sr);
					
					if((parseCount.getAndIncrement()) % 500000 == 0){
						System.out.println("|"+((float)parseCount.get())/((float)25000000));
					}
					
					
				}catch(RuntimeException e){
					errorCount++;
					System.out.printf("error on line %d, date %s, id: %s, total error count %d\n", parseCount.get(), s[1], ticker.getId(), errorCount);
					tickersWithErrors.add(ticker);
					continue;
				}
			}

			
			//Set<Ticker> throwAwayTickers = sortOutInactives(sorted, df.parse(mustContainDateForTicker));
			
			//analyzeThrowAways(throwAwayTickers, tickersWithErrors);
			
//			System.out.printf("must contain year: %s and date %s\n", mustContainYear, mustContainDateForTicker);
			
			int count = 0;
			for(Entry<Ticker, TreeSet<StockRecord>> s : sorted.entrySet()){
				for(StockRecord sr : s.getValue()){
					
					if((count) % 500000 == 0){
						System.out.println("|"+((float)count)/((float)parseCount.get()));
						w.flush();
					}
					
					w.write(String.format("\"%s\",\"%s\",\"%f\",\"%f\",\"%f\",\"%f\"\n", df.get().format(sr.getDate()), sr.getTicker().getId().toString(), sr.getAdjClose(), sr.getAdjVolume(), sr.getDividend(), sr.getSplitRatio()));
					count++;
				}
			}
			
//			int sumOfRecordCounts = 0;
//			int minNumberOfRecords = Integer.MAX_VALUE;
//			int maxNumberOfRecords = 0;
//			Ticker maxRecordTicker = null;
//			
//			Date earliest = new Date();
//			
//			HashMap<Date, HashMap<Ticker, StockRecord>> byDate = new HashMap<Date, HashMap<Ticker, StockRecord>>();
//			
//			
//			for(Entry<Ticker, TreeSet<StockRecord>> entry: sorted.entrySet()){
//				
//				System.out.printf("entry %s has %d records", entry.getKey().getId().toString(), entry.getValue().size());
//				sumOfRecordCounts += entry.getValue().size();
//				
//				if(entry.getValue().first().getDate().toInstant().isBefore(earliest.toInstant())){
//					earliest = entry.getValue().first().getDate();
//				}
//				
//				minNumberOfRecords = Math.min(entry.getValue().size(), minNumberOfRecords);
//				
//				if(entry.getValue().size() >= maxNumberOfRecords){
//					maxNumberOfRecords = entry.getValue().size();
//					maxRecordTicker = entry.getKey();
//				}
//				
//				for(StockRecord sr : entry.getValue()){
//					HashMap<Ticker, StockRecord> temp = null;
//					if((temp = byDate.get(sr.getDate())) == null){
//						temp = new HashMap<Ticker, StockRecord>();
//						byDate.put(sr.getDate(), temp);
//					}
//					temp.put(sr.getTicker(), sr);
//				}
//			}
//			
//			System.out.printf("loaded and sorted %d tickers, avg count of records: %f, max count: %d, min count %d, earliest date: %s\n",sorted.size(), (float)sumOfRecordCounts/(float)sorted.size(), maxNumberOfRecords, minNumberOfRecords, df.format(earliest));
//			System.out.printf("ended on file line number: %d, ticker count: %d,  %d errors\n", lastFileLineNumber, obfMap.size(), errorCount);
//
//			
//			HashMap<Date, HashMap<Ticker, Float>> percentByDate = new HashMap<Date, HashMap<Ticker, Float>>();
//			
//			Set<Date> datesThatExist = new TreeSet<Date>(new Comparator<Date>(){
//
//				@Override
//				public int compare(Date o1, Date o2) {
//					return o1.compareTo(o2);
//				}});
//			
//			datesThatExist.addAll(byDate.keySet());
//			
//			 HashMap<Ticker, StockRecord> previousDay = null;
//			 
//			for(Date da : datesThatExist){
//				 HashMap<Ticker, StockRecord> fd = byDate.get(da);
//				 if(previousDay == null){
//					 previousDay = fd;
//				 }else{
//					 
//					 for(Entry<Ticker, StockRecord> fdd : fd.entrySet()){
//						 StockRecord klk = null;
//						 if((klk = previousDay.get(fdd.getKey())) != null){
//							 
//							 float percent = ((fdd.getValue().getAdjClose()-klk.getAdjClose())/fdd.getValue().getAdjClose())*100.0f;
//							 
////							 System.out.printf("%s - %s to %s $%.2f to $%.2f %%change: %.2f\n",
////									 klk.getTicker().getId().toString(),
////									 df.format(klk.getDate()),
////									 df.format(fdd.getValue().getDate()),
////									 klk.getAdjClose(),
////									 fdd.getValue().getAdjClose(),
////									 percent);
//
//							 HashMap<Ticker,Float> bla = null;
//							 if((bla = percentByDate.get(fdd.getValue().getDate())) == null){
//								 bla = new HashMap<Ticker,Float>();
//								 percentByDate.put(fdd.getValue().getDate(), bla);
//							 }
//							 bla.put(fdd.getKey(),percent);
//							 
//							 
//							 
//						 }else{
//							 System.out.printf("starting coverage of %s on %s (no previous day)\n",fdd.getKey().getId().toString(), df.format(fdd.getValue().getDate()));
//						 }
//					 }
//					 previousDay = fd;
//				 }
//			}
//			HashMap<TickerXTicker,Float> same = new HashMap<TickerXTicker,Float>();
////			
//			Set<TickerXTicker> alreadyComparedToday = new HashSet<TickerXTicker>();
//			
//			TreeSet<Date> ssss = new TreeSet<Date>();
//			ssss.addAll(percentByDate.keySet());
//			
//			
//			for(Date date : ssss){
//				HashMap<Ticker, Float> a = percentByDate.get(date);
//				alreadyComparedToday.clear();
//				System.out.printf("looking at date %s, %d stocks to make comparisons of\n", df.format(date), a.size());
//				
//				for(Entry<Ticker, Float> entry : a.entrySet()){
//					Ticker t = entry.getKey();
//					for(Entry<Ticker, Float> entry2 : a.entrySet()){
//						
//						if(entry2.getKey().equals(t)){
//							
//						}else{
//
//							
//							TickerXTicker tt = new TickerXTicker(t,entry2.getKey());
//							if(!alreadyComparedToday.contains(tt)){
//
//								alreadyComparedToday.add(tt);
//
////								if(date.getDay() == 4){
//								//	System.out.println("comparing tickers "+tt+" for date: "+df.format(date)+" daty="+date.getDay());
////								}
//
//								if(entry.getValue() > 0 && entry2.getValue() > 0 || (entry.getValue() < 0 && entry2.getValue() < 0)){
////								if(entry.getValue().compareTo(entry2.getValue()) > 0){
//									Float fs = null;
//									if((fs = same.get(tt))==null){
//										fs = new Float(0);
//										same.put(tt, fs);
//									}else{
//												
//									}
//									same.put(tt, Float.valueOf(fs.floatValue()+1));
//								//	System.out.println("already found entry, incrementing since "+entry.getValue()+" and "+entry2.getValue()+" went the same way. new value: "+same.get(tt));
//
//								}else{
////								}else if(entry.getValue().compareTo(entry2.getValue()) < 0){
//									Float fs = null;
//									if((fs = same.get(tt))==null){
//										fs = new Float(0);
//										same.put(tt, fs);
//									}else{
//											//	System.out.println("already found entry, decrementing since "+entry.getValue()+" < "+entry2.getValue()+" old value: "+same.get(tt));
//										
//									}
//									same.put(tt, Float.valueOf(fs.floatValue()-1));
//									//System.out.println("already found entry, decrementing since "+entry.getValue()+" and "+entry2.getValue()+" didn't go the same way. new value: "+same.get(tt));
//								}
////								else{
////									
////								}
//							}
//						}
//					}	
//				}
//			}
//			
//			
//			TreeSet<TickerXTickerXFloat> ino = new TreeSet<TickerXTickerXFloat>(new Comparator<TickerXTickerXFloat>(){
//
//				@Override
//				public int compare(TickerXTickerXFloat o1, TickerXTickerXFloat o2) {
//					return o1.getF().compareTo(o2.getF());
//				}});
//			
//			for(Entry<TickerXTicker, Float> entry : same.entrySet()){
//				//System.out.printf("%s x %s %f\n", entry.getKey().getT1().getId().toString(), entry.getKey().getT2().getId().toString(), entry.getValue());
//				ino.add(new TickerXTickerXFloat(entry.getKey().getT1(), entry.getKey().getT2(), entry.getValue()));
//			}
//			
//			for(TickerXTickerXFloat ss : ino){
//				w.write(String.format("\"%s\",\"%s\",\"%f\"\n", ss.getT1().getId().toString(), ss.getT2().getId().toString(), ss.getF()));
//				
//			}
			
			
			
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}finally{
			lnr.close();
			w.close();
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

