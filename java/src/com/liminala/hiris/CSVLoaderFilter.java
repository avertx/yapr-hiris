package com.liminala.hiris;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeSet;

public class CSVLoaderFilter {


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


			int lastFileLineNumber = 0;
			lnr = new LineNumberReader(new FileReader(f));



			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			Integer maxLines = Integer.parseInt(args[3]);

			if(maxLines == -1) maxLines = Integer.MAX_VALUE;

			Integer maxMatches = Integer.parseInt(args[4]);


			TreeSet<TwoStockRecords> list = new TreeSet<TwoStockRecords>(compPercentThenVolume);


			Date d1 = df.parse("2016-01-07");
			Date d2 = df.parse("2016-01-08");


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


					if(!(d.equals(d1) || d.equals(d2))){
						continue;
					}


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



					StockRecord sr = new StockRecord(d, ticker, unadjOpen, unadjHigh, unadjLow, unadjClose, unadjVolume, dividend, splitRatio, adjOpen, adjHigh, adjLow, adjClose, adjVolume);


					if(records[1] != null && !records[1].getTicker().getSymbol().equals(ticker.getSymbol())){

						if(records[0] == null){
							records[1] = sr;
							continue;
						}

						Float diff = records[1].getAdjClose() - records[0].getAdjClose();

						Float percentChange = ((diff)/records[1].getAdjClose())*100;
						String chgSign = percentChange>0?"+":"";

						if(records[1].getAdjVolume() > MIN_VOLUME_TO_OUTPUT){

							TwoStockRecords rs = new TwoStockRecords();
							rs.setEarlier(records[0]);
							rs.setLater(records[1]);
							rs.setPercentChange(percentChange);

							list.add(rs);

							count++;
						//	System.out.printf("\t\t adding pair: %d %s %d\n", count, records[1].getTicker().getSymbol(), lastFileLineNumber);
							//System.out.printf("%s : %s / %s%.2f %5.2f%% %.0f\n",records[1].toString(), records[0].toString(), chgSign, diff, percentChange, records[1].getVolume());
						}
						//System.out.printf("record: %d\n", count);
						records[1] = null;
						records[0] = null;


					}

					if(records[1] != null){
						records[0] = records[1];
					}
					records[1] = sr;

					count++;
				}catch(RuntimeException e){
					errorCount++;
					System.out.printf("error on line %d, date %s, id: %s, total error count %d\n", count, s[1], ticker.getId(), errorCount);
					continue;
				}
			}



			System.out.printf("ended on file line number: %d, ticker count: %d,  %d errors\n", lastFileLineNumber, obfMap.size(), errorCount);



			String outHeader = String.format("\"symobf\", \"endDate\",\"price\",\"percentChange\",\"volume\"\n");
			w.write(outHeader);


			for(TwoStockRecords two : list){

				String uuid = obfMap.get(two.getLater().getTicker().getSymbol());
				if(uuid != null){
					String out = String.format("\"%s\",\"%s\",\"%.2f\",\"%.2f\",\"%.0f\"\n", uuid, df.format(two.getLater().getDate()), two.getLater().getAdjClose(), two.getPercentChange(), two.getLater().getAdjVolume());
					//String out = String.format("\"%s\",\"%.2f\",\"%.2f\",\"%.0f\"\n", two.getLater().getTicker().getId().toString(), two.getLater().getAdjClose(), two.getPercentChange(), two.getLater().getAdjVolume());
					w.write(out);

					System.out.print(out);
				}else{
					System.out.println("couldn't find obf for ticker: "+two.getLater().getTicker().getSymbol());	
				}
			}
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}finally{
			lnr.close();
			w.close();
			//			wObf.close();
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
