package com.liminala.hiris;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.apache.commons.lang3.time.FastDateFormat;

import ch.qos.logback.core.util.ExecutorServiceUtil;

public class CSVLoader4_1DayPercentCalcs {


	private static final Float MIN_VOLUME_TO_OUTPUT = 0.0f;

	private static float MAX_DAYS_MARKET_OPEN_PER_YEAR = 253.0f;
	private static ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>(){

		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd");
		}
	};
	

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


	public static void main(String[] args) throws IOException, ParseException {

		LineNumberReader lnr = null;
		FileWriter w = null;
		ExecutorService exec = null;

		try{
			File f = new File(args[0]);

			System.out.println("input file: "+f.getCanonicalPath());
			
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

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");


			String mustContainYear = args[3];
			String mustContainDate = args[4];
			
			int numThreads = Math.max(1, Runtime.getRuntime().availableProcessors()-2);
			
			
			ThreadGroup tg = new ThreadGroup(Thread.currentThread().getThreadGroup(), "tp");
			AtomicInteger tgCount = new AtomicInteger(0);

			
			ThreadFactory tf = new ThreadFactory(){

				@Override
				public Thread newThread(Runnable r) {
					DateFormatThread t = new DateFormatThread(tg,r,"-"+tgCount.getAndIncrement());
					return t;
					
				}};
				
			exec = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),tf);
					
//			Executors.newFixedThreadPool(numThreads);//
			AtomicInteger count = new AtomicInteger(0);
			//			while(count < maxMatches){
			AtomicInteger errorCount = new AtomicInteger(0);

			int numTickers = 10000;
			ConcurrentHashMap<Ticker, ConcurrentSkipListSet<StockRecord>> sorted = new ConcurrentHashMap<Ticker, ConcurrentSkipListSet<StockRecord>>(numTickers);

			Set<Ticker> tickersWithErrors = Collections.synchronizedSet(new HashSet<Ticker>());

			//			int corePoolSize = 4; 
			//			int maximumPoolSize = System. 
			//			long keepAliveTime = 1000*60*5; 
			//			TimeUnit unit = TimeUnit.MINUTES; 
			//			BlockingQueue<Runnable> workQueue;
			//			

			long startTS = System.currentTimeMillis();

//			List<Runnable> runnables = new ArrayList<>();
			ArrayList<Callable<Boolean>> callables = new ArrayList<Callable<Boolean>>();
			
			AtomicLong parseCount = new AtomicLong();
			
			String line = null;
			while((line = lnr.readLine()) != null){

				count.incrementAndGet();
				final String fLine = line;
				final int lineNum = lnr.getLineNumber();

//				Callable<Boolean> r = new Callable<Boolean>(){
//					
//					@Override
//					public Boolean call() {

						
//						System.out.println("parsing "+parseCount.getAndIncrement());
							final String[] s = fLine.split(",");


							Ticker ticker = new Ticker(s[0]);

							Date d = null;
							try {
								//								System.out.println("fLine and s[1]: "+fLine+" "+s[1]);
								 
								DateFormat df2 = DateFormatThread.dateFormat.get();
								if(df2 == null){
									df2 = new SimpleDateFormat("yyyy-MM-dd");
									DateFormatThread.dateFormat.set(df2);
								}
								d = df2.parse(s[1]);
								String year = s[1].split("-")[0];
								//								if(year.equals(mustContainYear)){
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

								SortedSet<StockRecord> set = null;
								
								set = sorted.computeIfAbsent(ticker, (hfd) -> new ConcurrentSkipListSet<StockRecord>(compDate));
								set.add(sr);

								if((parseCount.getAndIncrement()) % 100000 == 0){
									System.out.println("|"+((float)parseCount.get())/((float)25000000));
								}
								
							}catch(RuntimeException | ParseException e){
//																PrintWriter ps = new PrintWriter(System.out);
//																ps.write("s[1]: "+s[1]);
//																e.printStackTrace(ps);
//																ps.flush();
								errorCount.incrementAndGet();
								System.out.printf("error on line %d, date %s, id: %s, %s, \n%s, total error count %d\n", lineNum, s[1], ticker.getId(), fLine, e.getMessage(), errorCount.get());
								tickersWithErrors.add(ticker);
//								return false;
							}
//							return true;
					}
//				};
//
//				
//				callables.add(r);
//			}
			lastFileLineNumber = lnr.getLineNumber();

			
//			try {
//				System.out.println("shuffling "+callables.size()+" lines to parse");
//				Collections.shuffle(callables);
//				System.out.println("parsing all lines...");
//				List<Future<Boolean>> results = exec.invokeAll(callables);
//				
//				int countSuccess = 0;
//				int countFail = 0;
//				for(Future<Boolean> res : results){
//					try {
//						if(res.get()){
//							countSuccess++;
//						}else{
//							countFail++;
//						}
//					} catch (ExecutionException e1) {
//						// TODO Auto-generated catch block
//						countFail++;
//						e1.printStackTrace();
//					}
//				}
//				System.out.println("done parsing lines, "+countSuccess+" success, "+countFail+" failures");
//				
//			} catch (InterruptedException e1) {
//				throw new RuntimeException(e1);
//			}
			


			System.out.println("parsed "+lastFileLineNumber+" in "+(System.currentTimeMillis()-startTS)+" ms");

//			LinkedHashMap<String,String> yearDateLooper = new LinkedHashMap<String,String>();
//			
//			yearDateLooper.put("1980","1980-12-31");
//			yearDateLooper.put("1981","1981-12-31");
//			yearDateLooper.put("1982","1982-12-31");
//			yearDateLooper.put("1983","1983-12-30");
//			yearDateLooper.put("1984","1984-12-31");
//			yearDateLooper.put("1985","1985-12-31");
//			yearDateLooper.put("1986","1986-12-31");
//			yearDateLooper.put("1987","1987-12-31");
//			yearDateLooper.put("1988","1988-12-30");
//			yearDateLooper.put("1989","1989-12-29");
//			yearDateLooper.put("1990","1990-12-31");
//			yearDateLooper.put("1991","1991-12-31");
//			yearDateLooper.put("1992","1992-12-31");
//			
//			yearDateLooper.put("1993","1993-12-31");
//			yearDateLooper.put("1994","1994-12-30");
//			yearDateLooper.put("1995","1995-12-29");
//			yearDateLooper.put("1996","1996-12-31");
//			yearDateLooper.put("1997","1997-12-31");
//			yearDateLooper.put("1998","1998-12-31");
//			yearDateLooper.put("1999","1999-12-31");
//			yearDateLooper.put("2000","2000-12-29");
//			yearDateLooper.put("2001","2001-12-31");
//			yearDateLooper.put("2002","2002-12-31");
//			yearDateLooper.put("2003","2003-12-31");
//			yearDateLooper.put("2004","2004-12-31");
//			yearDateLooper.put("2005","2005-12-30");
//			yearDateLooper.put("2006","2006-12-29");
//			yearDateLooper.put("2007","2007-12-31");
//			yearDateLooper.put("2008","2008-12-31");
//			yearDateLooper.put("2009","2009-12-31");
//			yearDateLooper.put("2010","2010-12-31");
//			yearDateLooper.put("2011","2011-12-30");
//			yearDateLooper.put("2012","2012-12-31");
//			yearDateLooper.put("2013","2013-12-31");
//			yearDateLooper.put("2014","2014-12-31");
//			yearDateLooper.put("2015","2015-12-31");
			
			
			HashMap<String, DatedStockRecordsWithMetadata> allYears = new HashMap<String, DatedStockRecordsWithMetadata>();
//			HashMap<String, HashMap<TickerXTicker,Float>> allYearsCross = new HashMap<String, HashMap<TickerXTicker,Float>>(); 
			
//			for(Entry<String, String> ent : yearDateLooper.entrySet()){

//				mustContainYear = ent.getKey();
//				mustContainDate = ent.getValue();
				
				
				File outDir = new File(args[1]);
				if(outDir.exists() && outDir.isDirectory()){
					System.out.println("output directory: "+outDir.getCanonicalPath());
				}else{
					throw new RuntimeException("second arg, output directory either doesn't exist or is not a directory: "+args[1]);
				}
				
				String fl[] = f.getCanonicalPath().split(File.separator);
				String filename = fl[fl.length-1];
				
				File output = new File(args[1]+"PerChanges-1day-"+filename.split("\\.")[0]+".csv");
				
				System.out.println("output file: "+output.getCanonicalPath());
				if(output.exists()){
					System.out.println("deleting existing output file: "+output.getCanonicalPath());
					output.delete();
				}
				System.out.println("creating output file: "+output.getCanonicalPath());

				output.createNewFile();
				w = new FileWriter(output);

				try{
					

					HashMap<Ticker, SortedSet<StockRecord>> deepCopyDateFilteredSorted = new HashMap<Ticker, SortedSet<StockRecord>>();
					
					for(Entry<Ticker, ConcurrentSkipListSet<StockRecord>> e : sorted.entrySet()){
						TreeSet<StockRecord> ts = new TreeSet<StockRecord>(compDate);
						ts.addAll(e.getValue());
						deepCopyDateFilteredSorted.put(e.getKey(), ts);
					}
					
					//Set<Ticker> thrownAwayTickersBCYear = sortOutYear(deepCopyDateFilteredSorted, Integer.valueOf(mustContainYear));
					
				//	Set<Ticker> throwAwayTickers = sortOutInactives(deepCopyDateFilteredSorted, df.parse(mustContainDate));

				//	analyzeThrowAways(throwAwayTickers, tickersWithErrors);

					startTS = System.currentTimeMillis();
					DatedStockRecordsWithMetadata byDate = sortByDate(obfMap, lastFileLineNumber, df, errorCount, deepCopyDateFilteredSorted);
					System.out.println("sorted by date "+(System.currentTimeMillis()-startTS)+" ms");
					
					allYears.put(mustContainYear, byDate);
					
					Set<Date> datesThatExist = new TreeSet<Date>(new Comparator<Date>() {

						@Override
						public int compare(Date o1, Date o2) {
							return o1.compareTo(o2);
						}});

					
					datesThatExist.addAll(byDate.getByDate().keySet());

					Set<Ticker> errors = new HashSet<Ticker>();
					
					startTS = System.currentTimeMillis();
					HashMap<Date, HashMap<Ticker, Float>> percentByDate = calc1DayPercentChanges(byDate.getByDate(), datesThatExist, errors);
					
					for(Entry<Date, HashMap<Ticker, Float>> pp : percentByDate.entrySet()){
							for(Entry<Ticker, Float> sd : pp.getValue().entrySet()){
								w.write("\""+dateFormat.get().format(pp.getKey())+"\",\""+sd.getKey().getId().toString()+"\",\""+sd.getValue()+"\",\""+(errors.contains(sd.getKey())?"1":"0")+"\"\n");
							}
					}
//					System.out.printf("%d combinations to output, starting output to file %s\n", same.size(), output.getCanonicalPath());
//					for(Entry<TickerXTicker, LongAdder> entry : same.entrySet()){
//						if((h++) % (same.size()/100) == 0){
//							System.out.println("|"+((float)h)/((float)same.size()));
//						}
//						int s1 = deepCopyDateFilteredSorted.get(entry.getKey().getT1()).size();
//						int s2 = deepCopyDateFilteredSorted.get(entry.getKey().getT2()).size();
//						int numDaysCompared = Math.min(s1, s2);
//						float movedTogetherness = ((float)entry.getValue().longValue())/((float)numDaysCompared);
//						float weightBasedOnNumSamples = ((float)numDaysCompared)/MAX_DAYS_MARKET_OPEN_PER_YEAR;
//						float strengthOfTogethernessAfterWeightAdjusted = weightBasedOnNumSamples*movedTogetherness; 
//						w.write(String.format("\"%s\",\"%s\",\"%f\",\"%f\",\"%d\",\"%f\",\"%f\"\n", entry.getKey().getT1().getId().toString(), entry.getKey().getT2().getId().toString(), entry.getValue().floatValue(), movedTogetherness, numDaysCompared, weightBasedOnNumSamples, strengthOfTogethernessAfterWeightAdjusted));
//						//ino.add(new TickerXTickerXFloat(new TickerXTicker(entry.getKey().getT1(), entry.getKey().getT2()), entry.getValue().floatValue()));
//					}
				}finally{
					w.flush();
					w.close();
				}
//			}
			
			for(Entry<String, DatedStockRecordsWithMetadata> year : allYears.entrySet()){
				
				
			}
			
			
//			Set<TickerXTicker> interesting = new HashSet<TickerXTicker>();
//			
//			
//			
//			//(\d+), (.+), (.+), .+
//			//interesting.add(new TickerXTicker(new Ticker(UUID.fromString("$2")), new Ticker(UUID.fromString("$3"))));
//			try{
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("1b2a8504-aaa3-4a0a-b85b-6b94356afab6")), new Ticker(UUID.fromString("a34eff91-9e69-4c54-9bd8-d6440b67025c"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("c39ef8a5-b470-4aa2-85e8-b49bc2d04643")), new Ticker(UUID.fromString("c63405da-da89-4cc3-ac68-c70271ab8e59"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("8ff07213-0d63-4adb-a450-08f23003abf9")), new Ticker(UUID.fromString("9e123cdd-6760-45ae-ae8f-6decb8121735"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("3ca947c2-9aa4-472e-9a81-8d151a52a786")), new Ticker(UUID.fromString("d9af087e-e085-472f-bc3e-c78cd4d74d4e"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("30108312-fb3d-4e02-9709-ca005a36e244")), new Ticker(UUID.fromString("b36ecf97-6104-488c-b15c-bcf305f657a4"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("2374f9c9-5348-4fa6-831a-119c7b066863")), new Ticker(UUID.fromString("833bebea-c854-4245-acf7-7e243d3f2abe"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("5604a89e-53f1-423f-be90-7c4639de1a6d")), new Ticker(UUID.fromString("b7f4ff40-24ea-46f9-8d46-3bee24e07edf"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("4e433ff6-541b-467e-acb8-5ce72260254c")), new Ticker(UUID.fromString("e77db502-8ae1-4494-beff-d4cd567c80f0"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("00740ac4-3b99-44aa-85d0-9a9619094a63")), new Ticker(UUID.fromString("b0108265-bb28-4665-a8f9-cadef227353c"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("2374f9c9-5348-4fa6-831a-119c7b066863")), new Ticker(UUID.fromString("98e033c2-d42a-4857-a1f4-32720b34fe49"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("761c6294-5a65-4ba0-8a47-9f55776f3873")), new Ticker(UUID.fromString("ea140a23-848b-45ea-8493-c594c22af178"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("00f947f3-fcdc-426c-87e8-f1c61cd9f04c")), new Ticker(UUID.fromString("91a0d2a8-6492-4d97-a9f0-bd12161880dd"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("2c82c7fe-b123-4396-8172-5093888cda18")), new Ticker(UUID.fromString("c81d36d8-f193-44e2-92b3-5a4d0913015d"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("6a079892-9787-4e90-971c-2fbbfb28f9c5")), new Ticker(UUID.fromString("91a0d2a8-6492-4d97-a9f0-bd12161880dd"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("185f9921-98e7-491b-8108-5fb137d055bf")), new Ticker(UUID.fromString("85ac641f-8748-42cb-ac60-dc2e1853579b"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("05ec6f8d-fca7-4592-a3ec-83af66fb4376")), new Ticker(UUID.fromString("b64d20a2-c981-4e1c-b1cd-60dac6d4cbf2"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("2c82c7fe-b123-4396-8172-5093888cda18")), new Ticker(UUID.fromString("7ad5df55-5819-4e15-bd5d-aa0a2f219428"))));
//				interesting.add(new TickerXTicker(new Ticker(UUID.fromString("7ad5df55-5819-4e15-bd5d-aa0a2f219428")), new Ticker(UUID.fromString("c81d36d8-f193-44e2-92b3-5a4d0913015d"))));
//			
//			} catch (NoTickerForUUIDException e) {
//				e.printStackTrace();
//				Runtime.getRuntime().exit(1);
//			}
//			
//
//			
//			
//			
//			System.out.print(String.format("writing sub-data, by date, for %d comparisons\n",ino.size()));
//			for(TickerXTickerXFloat ss : ino){
//
//				if(!interesting.contains(ss.getTt())) continue;
//				
//				w.write(String.format("\"%s\",\"%s\",\"%f\"\n", ss.getTt().getT1().getId().toString(), ss.getTt().getT2().getId().toString(), ss.getF()));
//
//				for(Date date : ssss){
//
//					HashMap<Ticker, Float> perc = percentByDate.get(date);
//
//					Float h1 = perc.get(ss.getTt().getT1());
//					Float h2 = perc.get(ss.getTt().getT2());
//
//					HashMap<Ticker,StockRecord> h = byDate.get(date);
//					StockRecord k1 = h.get(ss.getTt().getT1());
//					StockRecord k2 = h.get(ss.getTt().getT2());
//
//					
//
//					if(h1 != null && h2 != null && k1 != null && k2 != null){
//
//						w.write(String.format("\t%s [%f %f] %f %f {%f %f} %s\n", 
//								df.format(date), h1, h2, k1.getAdjClose(), k2.getAdjClose(), k1.getAdjVolume(), k2.getAdjVolume(),
//								countedPerDayUp.get(date).contains(new TickerXTicker(ss.getTt().getT1(),ss.getTt().getT2()))?"+1":countedPerDayDown.get(date).contains(new TickerXTicker(ss.getTt().getT1(),ss.getTt().getT2()))?"-1":"0"
//								));
//					}else{
//						w.write(df.format(date)+" incomplete data");
//					}
//				}
//				w.flush();
//			}
			
			
			//			
			//			System.out.print(String.format("[writing] number of sorted comparisons: %d\n",ino.size()));
			//			w.write(String.format("number of comparisons: %d\n",ino.size()));
			//			
			//			for(TickerXTickerXFloat ss : ino){
			//				
			//				w.write(String.format("\"%s\",\"%s\",\"%f\"\n", ss.getTt().getT1().getId().toString(), ss.getTt().getT2().getId().toString(), ss.getF()));
			//			}

		
		}finally{
			
			exec.shutdown();
			lnr.close();
			
			
			//			wObf.close();
		}
	}

	private static Set<Ticker> sortOutYear(HashMap<Ticker, SortedSet<StockRecord>> copy, Integer year) {
		
		Set<Ticker> tickersToRemove = new HashSet<Ticker>();
		for(Entry<Ticker, SortedSet<StockRecord>> e : copy.entrySet()){
			
			Set<StockRecord> toRemove = new HashSet<StockRecord>();
			
			boolean foundSRInYear = false;
			for(StockRecord sr : e.getValue()){
				
				GregorianCalendar cal = (GregorianCalendar) Calendar.getInstance();
				cal.setTime(sr.getDate());
				if(cal.get(Calendar.YEAR) != year){
					toRemove.add(sr);
				}else{
					foundSRInYear = true;
				}
			}
			if(e.getValue().size() == toRemove.size()){
				tickersToRemove.add(e.getKey());
			}else{
				e.getValue().removeAll(toRemove);
			}
			
			if(!foundSRInYear){
				tickersToRemove.add(e.getKey());
			}
			
		}
		System.out.println(tickersToRemove.size()+" tickers not found in year "+year);
		copy.keySet().removeAll(tickersToRemove);
		System.out.println(copy.keySet().size()+" tickers left in year "+year);
		
		return tickersToRemove;
	}

	private static List<Callable<Boolean>> crossAllCountUpDownSimilarity(
			HashMap<Date, HashMap<Ticker, Float>> percentByDate, 
			ConcurrentHashMap<TickerXTicker,LongAdder>/*HashMap<TickerXTicker, Float>*/ same,
			/*HashMap<Date, HashSet<TickerXTicker>> countedPerDayUp,
			HashMap<Date, HashSet<TickerXTicker>> countedPerDayDown,*/
			TreeSet<Date> ssss) {
		
		List<Callable<Boolean>> calls = new ArrayList<Callable<Boolean>>();
		
		System.out.println(ssss.size()+" dates to look at");
		for(final Date date : ssss){

			Callable<Boolean> r = new Callable<Boolean>(){

				@Override
				public Boolean call() {
					// TODO Auto-generated method stub


					//			countedPerDayUp.put(date, new HashSet<TickerXTicker>());
					//			countedPerDayDown.put(date, new HashSet<TickerXTicker>());

					HashMap<Ticker, Float> a = percentByDate.get(date);

					System.out.printf("looking at date %s, %d stocks to make comparisons of\n", df().format(date), a.size());

					HashMap<Ticker, Float> leftToCompare = new HashMap<Ticker, Float>();
					leftToCompare.putAll(percentByDate.get(date));

					for(Entry<Ticker, Float> entry : a.entrySet()){

						Ticker t = entry.getKey();

						leftToCompare.remove(t);

						//					System.out.println("looking at "+t.getId().toString()+", "+leftToCompare.size()+" comparisons left on "+df.format(date));

						for(Entry<Ticker, Float> entry2 : leftToCompare.entrySet()){

							if(entry2.getKey().equals(t)){

							}else{


								TickerXTicker tt = new TickerXTicker(t,entry2.getKey());
								//							System.out.println("\tcomparing "+tt.toString()+" on "+df.format(date));

								if((entry.getValue() > 0 && entry2.getValue() > 0) || (entry.getValue() < 0 && entry2.getValue() < 0)){

									Float fs = null;

									same.computeIfAbsent(tt, (k) -> new LongAdder()).increment();

									//							if((fs = same.get(tt))==null){
									//								
									//								fs = new Float(0);
									//								same.put(tt, fs);
									//							}else{
									//
									//							}
									//							same.put(tt, Float.valueOf(fs.floatValue()+1));
									//	System.out.println("already found entry, incrementing since "+entry.getValue()+" and "+entry2.getValue()+" went the same way. new value: "+same.get(tt));

									//							countedPerDayUp.get(date).add(tt);


								}else if(entry.getValue() == 0 || entry2.getValue() == 0){

								}else{

									same.computeIfAbsent(tt, (k) -> new LongAdder()).decrement();

									//							Float fs = null;
									//							if((fs = same.get(tt))==null){
									//								fs = new Float(0);
									//								same.put(tt, fs);
									//							}else{
									//								//	System.out.println("already found entry, decrementing since "+entry.getValue()+" < "+entry2.getValue()+" old value: "+same.get(tt));
									//
									//							}
									//							same.put(tt, Float.valueOf(fs.floatValue()-1));
									//							countedPerDayDown.get(date).add(tt);
									//System.out.println("already found entry, decrementing since "+entry.getValue()+" and "+entry2.getValue()+" didn't go the same way. new value: "+same.get(tt));
								}
							}
						}	
					}
					return true;
				}
			};
			calls.add(r);
		}
		return calls;
	}

	private static HashMap<Date, HashMap<Ticker, Float>> calc1DayPercentChanges(HashMap<Date, HashMap<Ticker, StockRecord>> byDate, Set<Date> datesThatExist, Set<Ticker> errors) {
		
		HashMap<Ticker, StockRecord> previousDay = null;
		HashMap<Date, HashMap<Ticker, Float>> percentByDate = new HashMap<Date, HashMap<Ticker, Float>>();
		
		Set<Ticker> startedCoverage = new HashSet<Ticker>();

		for(Date da : datesThatExist){
			
			HashMap<Ticker, StockRecord> fd = byDate.get(da);
			if(previousDay == null){
				previousDay = fd;
				System.out.println("starting coverage with "+fd.size()+" stocks on "+df().format(da));
			}else{

				for(Entry<Ticker, StockRecord> fdd : fd.entrySet()){
					StockRecord klk = null;
					if((klk = previousDay.get(fdd.getKey())) != null){

						float percent = ((fdd.getValue().getAdjClose()-klk.getAdjClose())/fdd.getValue().getAdjClose())*100.0f;

						//							 System.out.printf("%s - %s to %s $%.2f to $%.2f %%change: %.2f\n",
						//									 klk.getTicker().getId().toString(),
						//									 df.format(klk.getDate()),
						//									 df.format(fdd.getValue().getDate()),
						//									 klk.getAdjClose(),
						//									 fdd.getValue().getAdjClose(),
						//									 percent);

						HashMap<Ticker,Float> bla = null;
						if((bla = percentByDate.get(fdd.getValue().getDate())) == null){
							bla = new HashMap<Ticker,Float>();
							percentByDate.put(fdd.getValue().getDate(), bla);
						}
						bla.put(fdd.getKey(),percent);

					}else{


						if(startedCoverage.contains(fdd.getKey())){
							System.out.println("previously started coverage of "+fdd.getKey()+", input must be missing data in between");
							errors.add(fdd.getKey());
						}else{
							System.out.printf("starting coverage of %s on %s (no previous day)\n",fdd.getKey().getId().toString(), df().format(fdd.getValue().getDate()));
							startedCoverage.add(fdd.getKey());
						}
					}
				}
				previousDay = fd;
			}
		}
		return percentByDate;
	}

	private static DatedStockRecordsWithMetadata sortByDate(HashMap<String, String> obfMap,
			int lastFileLineNumber, DateFormat df, AtomicInteger errorCount,
			HashMap<Ticker, SortedSet<StockRecord>> sorted) {
		
		DatedStockRecordsWithMetadata ret = new DatedStockRecordsWithMetadata();
		
		int sumOfRecordCounts = 0;
		int minNumberOfRecords = Integer.MAX_VALUE;
		int maxNumberOfRecords = 0;
		Ticker maxRecordTicker = null;

		Date earliest = new Date();

		//HashMap<Date, HashMap<Ticker, StockRecord>> byDate = new HashMap<Date, HashMap<Ticker, StockRecord>>();


		for(Entry<Ticker, SortedSet<StockRecord>> entry: sorted.entrySet()){

			System.out.printf("entry %s has %d records\n", entry.getKey().getId().toString(), entry.getValue().size());
			sumOfRecordCounts += entry.getValue().size();

			if(entry.getValue().first().getDate().toInstant().isBefore(earliest.toInstant())){
				earliest = entry.getValue().first().getDate();
			}

			minNumberOfRecords = Math.min(entry.getValue().size(), minNumberOfRecords);

			if(entry.getValue().size() >= maxNumberOfRecords){
				maxNumberOfRecords = entry.getValue().size();
				maxRecordTicker = entry.getKey();
			}

			for(StockRecord sr : entry.getValue()){
				HashMap<Ticker, StockRecord> temp = null;
				if((temp = ret.getByDate().get(sr.getDate())) == null){
					temp = new HashMap<Ticker, StockRecord>();
					ret.getByDate().put(sr.getDate(), temp);
				}
				temp.put(sr.getTicker(), sr);
			}
		}

		ret.setMaxNumberOfRecords(maxNumberOfRecords);
		ret.setMaxRecordTicker(maxRecordTicker);
		
		System.out.printf("loaded and sorted %d tickers, avg count of records: %f, max count: %d, min count %d, earliest date: %s\n",sorted.size(), (float)sumOfRecordCounts/(float)sorted.size(), maxNumberOfRecords, minNumberOfRecords, df.format(earliest));
		System.out.printf("ended on file line number: %d, ticker count: %d,  %d errors\n", lastFileLineNumber, obfMap.size(), errorCount.get());
		
		return ret;
	}

	private static void analyzeThrowAways(Set<Ticker> throwAwayTickers, Set<Ticker> tickersWithErrors) {


		System.out.printf("%d tickers to throw away due to incorrect date, %d with parsing errors\n", throwAwayTickers.size(), tickersWithErrors.size());
		throwAwayTickers.removeAll(tickersWithErrors);

		System.out.printf("after removing tickers with errors, number of throw away tickers remaining: %d\n",throwAwayTickers.size());

	}

	private static Set<Ticker> sortOutInactives(HashMap<Ticker, SortedSet<StockRecord>> sorted, Date d) {

		System.out.println("sorting out stocks whose last record is not on "+d);

		Set<Ticker> throwAwayTickers = new HashSet<Ticker>();
		for(Entry<Ticker, SortedSet<StockRecord>> entry : sorted.entrySet()){
			if(entry.getValue().last().getDate().compareTo(d) != 0 ){
				throwAwayTickers.add(entry.getKey());
			}
		}

		System.out.printf("throwing away %d tickers, due to no records on date:%s\n", throwAwayTickers.size(), df().format(d));
		sorted.keySet().removeAll(throwAwayTickers);

		return throwAwayTickers;

	}

	public static DateFormat df(){
		return CSVLoader4_1DayPercentCalcs.dateFormat.get();
	}
	
	public static HashMap<String, String> getObfMap(File f) {

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
