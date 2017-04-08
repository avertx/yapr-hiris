package com.liminala.hiris;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

public class CSVLoaderGenerateDBandOBFMap {


	private static final Float MIN_VOLUME_TO_OUTPUT = 0.0f;

	public static void main(String[] args) throws IOException, ParseException {

	
		File f = new File(args[0]);

		File output = new File(args[1]);
		if(output.exists()){
			output.delete();
		}
		output.createNewFile();
		FileWriter w = new FileWriter(output);
		
//		File outputRaw = new File(args[2]);
//		if(outputRaw.exists()){
//			outputRaw.delete();
//		}
//		outputRaw.createNewFile();
//		FileWriter wRaw = new FileWriter(outputRaw);
		
		File outputObf = new File(args[2]);
		if(outputObf.exists()){
			outputObf.delete();
		}
		outputObf.createNewFile();
		FileWriter wObf = new FileWriter(outputObf);
		
		HashSet<Ticker> obfmap = new HashSet<Ticker>();
	
		int lastFileLineNumber = 0;
		LineNumberReader lnr = new LineNumberReader(new FileReader(f));
		try{


			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			Integer maxLines = Integer.parseInt(args[3]);

			if(maxLines == -1) maxLines = Integer.MAX_VALUE;

			Integer maxMatches = Integer.parseInt(args[4]);

			Comparator<TwoStockRecords> compPercentThenVolume = new Comparator<TwoStockRecords>(){

				@Override
				public int compare(TwoStockRecords o1, TwoStockRecords o2) {

					int pc = 0;
					pc = o1.getPercentChange().compareTo(o2.getPercentChange());

					if(pc != 0) return pc;

					return o1.getLater().getAdjVolume().compareTo(o2.getLater().getAdjVolume());
				}

			};
			TreeSet<TwoStockRecords> list = new TreeSet<TwoStockRecords>(compPercentThenVolume);


//			Date d1 = df.parse("2015-12-31");
//			Date d2 = df.parse("2016-01-04");


			StockRecord[] records = new StockRecord[2];
			int count = 0;
//			while(count < maxMatches){
			int errorCount = 0;

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


					//				if(!(d.equals(d1) || d.equals(d2))){
					//					continue;
					//				}


					Float unadjOpen = Float.parseFloat(s[2]);
					Float unadjHigh = Float.parseFloat(s[3]);
					Float unadjLow = Float.parseFloat(s[4]);
					Float unadjClose = Float.parseFloat(s[5]);
					Float unadjVolume = Float.parseFloat(s[6]);

					Float dividend = Float.parseFloat(s[7]);
					Float splitRatio = Float.parseFloat(s[8]);


					Float adjOpen = Float.parseFloat(s[9]);
					Float adjHigh = Float.parseFloat(s[10]);
					Float adjLow = Float.parseFloat(s[11]);
					Float adjClose = Float.parseFloat(s[12]);
					Float adjVolume = Float.parseFloat(s[13]);



					String o = String.format("\"%s\",\"%s\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\",\"%f\"\n", ticker.getId().toString(), s[1], unadjOpen, unadjHigh, unadjLow, unadjClose, unadjVolume, dividend, splitRatio, adjOpen, adjHigh, adjLow, adjClose, adjVolume);
					w.write(o);

					//				System.out.println(count+++" "+o);
					obfmap.add(ticker);
					

				}catch(RuntimeException e){
					errorCount++;
					System.out.printf("error on line %d, date %s, id: %s, total error count %d\n", count, s[1], ticker.getId(), errorCount);
					continue;
				}
//				StockRecord sr = new StockRecord(d, ticker, unadjOpen, unadjHigh, unadjLow, unadjClose, unadjVolume, dividend, splitRatio, adjOpen, adjHigh, adjLow, adjClose, adjVolume);

	
//				if(records[1] != null && !records[1].getTicker().getSymbol().equals(ticker.getSymbol())){
//
//					if(records[0] == null){
//						records[1] = sr;
//						continue;
//					}
//
//					Float diff = records[1].getAdjClose() - records[0].getAdjClose();
//
//					Float percentChange = ((diff)/records[1].getAdjClose())*100;
//					String chgSign = percentChange>0?"+":"";
//
//					if(records[1].getAdjVolume() > MIN_VOLUME_TO_OUTPUT){
//
//						TwoStockRecords rs = new TwoStockRecords();
//						rs.setEarlier(records[0]);
//						rs.setLater(records[1]);
//						rs.setPercentChange(percentChange);
//
//						list.add(rs);
//
//						count++;
//						System.out.printf("\t\t adding pair: %d %s %d\n", count, records[1].getTicker().getSymbol(), lastFileLineNumber);
//						//System.out.printf("%s : %s / %s%.2f %5.2f%% %.0f\n",records[1].toString(), records[0].toString(), chgSign, diff, percentChange, records[1].getVolume());
//					}
//					System.out.printf("record: %d\n", count);
//					records[1] = null;
//					records[0] = null;
//
//
//				}
//
//				if(records[1] != null){
//					records[0] = records[1];
//				}
				//				records[1] = sr;

				count++;
			}

			System.out.printf("starting write of obfmap, %d entries\n", obfmap.size());
			for(Ticker ticker : obfmap){
				String m = String.format("\"%s\",\"%s\"\n", ticker.getId(), ticker.getSymbol());
				wObf.write(m);
			}

			System.out.printf("ended on file line number: %d, ticker count: %d,  %d errors\n", lastFileLineNumber, obfmap.size(), errorCount);



//			String outHeader = String.format("\"symobf\",\"price\",\"percentChange\",\"volume\"\n");
//			w.write(outHeader);
//
//
//			for(TwoStockRecords two : list){
//
//				String out = String.format("\"%s\",\"%.2f\",\"%.2f\",\"%.0f\"\n", two.getLater().getTicker().getId().toString(), two.getLater().getAdjClose(), two.getPercentChange(), two.getLater().getAdjVolume());
//				//String out = String.format("\"%s\",\"%.2f\",\"%.2f\",\"%.0f\"\n", two.getLater().getTicker().getId().toString(), two.getLater().getAdjClose(), two.getPercentChange(), two.getLater().getAdjVolume());
//				w.write(out);
//
//				System.out.print(out);
//				//System.out.printf("%d %s %.2f%%\n",i++, two.getLater(), two.getPercentChange());
//
//
//			}
//		} catch (ParseException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
		}finally{
			lnr.close();
			w.close();
			wObf.close();
		}
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
				
				ret.put(obf, symbol);
			}
			
			System.out.printf("loaded %d obf symbols from file %s", ret.size(), f.getCanonicalPath());
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
