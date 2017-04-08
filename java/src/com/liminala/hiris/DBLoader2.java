package com.liminala.hiris;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.FileImageOutputStream;
import javax.swing.JFrame;

import com.liminala.woodworking.ui.ImageSaver;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class DBLoader2 {

	public static void main(String[] args) {
		
		int outImageW = 60000;
		int outImageH = 35000;
		if((long)outImageW*(long)outImageH > Integer.MAX_VALUE) throw new RuntimeException("output image too large");
		ExecutorService exec = null;
		
		File obfMapFile = new File(args[0]);
		try {
			System.out.println("using obf map file file: "+obfMapFile.getCanonicalPath());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if(!obfMapFile.exists()){
			try {
				throw new RuntimeException("obfmapfile doesn't exist: "+obfMapFile.getCanonicalPath());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		File output = new File(args[1]);
		if(output.exists()){
			output.delete();
		}
		try {
			output.createNewFile();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		HashMap<String, String> obfMap = CSVLoader3.getObfMap(obfMapFile);
		for(Entry<String, String> e : obfMap.entrySet()){
			Ticker.getUuidForTicker().put(e.getKey(), UUID.fromString(e.getValue()));
		}
		for(Entry<String, String> e : obfMap.entrySet()){
			Ticker.getTickerForUUID().put(UUID.fromString(e.getValue()), e.getKey());
		}
		
		int numThreads = Math.max(1, (Runtime.getRuntime().availableProcessors()-2)/2);
		
		
		ThreadGroup tg = new ThreadGroup(Thread.currentThread().getThreadGroup(), "tp");
		AtomicInteger tgCount = new AtomicInteger(0);

		
		ThreadFactory tf = new ThreadFactory(){

			@Override
			public Thread newThread(Runnable r) {
				DateFormatThread t = new DateFormatThread(tg,r,"-"+tgCount.getAndIncrement());
				return t;
				
			}};
			
		exec = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),tf);
		
		Connection conn = null;
		
		try {
			try {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		    conn = DriverManager.getConnection("jdbc:mysql://192.168.3.115:3306/hiris?user=m&password=m");

		    
		    String[] yearsToLoad = new String[]{

		    		"1980","1981","1982","1983","1984","1985","1986","1987","1988","1989",
		    		"1990","1991","1992","1993","1994","1995","1996","1997","1998","1999",//};//,
		    		"2000","2001","2002","2003","2004","2005","2006","2007","2008","2009"};//,
		    		//"2010","2011","2012","2013","2014","2015"};

		    String[] years = new String[]{
		    		"1980","1981","1982","1983","1984","1985","1986","1987","1988","1989",
		    		"1990","1991","1992","1993","1994","1995","1996","1997","1998","1999",
		    		"2000","2001","2002","2003","2004","2005","2006","2007","2008","2009"};//,
		    		//"2010","2011","2012","2013","2014","2015"};
		    
		    AtomicInteger ai = new AtomicInteger(yearsToLoad.length);
		    
		    
		    HashMap<String, PreparedStatement> statements = new HashMap<>();
		    
		    for(String year : years){
		    	PreparedStatement ps = conn.prepareStatement("select obf1,obf2,weightedTogetherness from PC_"+year+" where obf1 = ? and obf2 = ?");
		    	statements.put(year, ps);
		    }
		    
		    HashMap<String, TreeSet<TickerXTickerXFloat>> fdsa = new HashMap<String, TreeSet<TickerXTickerXFloat>>();
		    
		    ConcurrentHashMap<String, ConcurrentHashMap<TickerXTicker,Float>> fdds = new ConcurrentHashMap<String, ConcurrentHashMap<TickerXTicker,Float>>();
		    
		    long startTS1 = System.currentTimeMillis();
		    //		    long startTS = System.currentTimeMillis();

		    ArrayList<Callable<Boolean>> calls = new ArrayList<Callable<Boolean>>();
		    
		    for(String year : yearsToLoad){

		    	
		    	Callable<Boolean> call = new Callable<Boolean>(){

		    		@Override
		    		public Boolean call() throws Exception {

		    			Connection con = DriverManager.getConnection("jdbc:mysql://192.168.3.115:3306/hiris?user=m&password=m");

		    			try{
		    				long startTS = System.currentTimeMillis();
		    				System.out.println("loading "+year);
		    				PreparedStatement ps = con.prepareStatement("select obf1,obf2,weightedTogetherness from PC_"+year);
		    				ResultSet rs = ps.executeQuery();

		    				ConcurrentHashMap<TickerXTicker,Float> hm = new ConcurrentHashMap<TickerXTicker,Float>();

		    				fdds.put(year, hm);

		    				while(rs.next()){
		    					try {

		    						TickerXTicker tt = new TickerXTicker(new Ticker(UUID.fromString(rs.getString(1))), new Ticker(UUID.fromString(rs.getString(2))));
		    						hm.put(tt, rs.getFloat(3));


		    					} catch (NoTickerForUUIDException e1) {
		    						// TODO Auto-generated catch block
		    						e1.printStackTrace();
		    					}
		    				}
		    				System.out.println("loaded year "+year+" in "+(System.currentTimeMillis()-startTS)+", "+ai.getAndDecrement()+" left");
		    				// TODO Auto-generated method stub
		    			}finally{
		    				con.close();
		    			}
		    			return true;
		    		}
		    	};
		    	calls.add(call);
		    }
		    
		    try {
				exec.invokeAll(calls);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		    exec.shutdown();
		    
		    System.out.println("loaded all years in "+(System.currentTimeMillis()-startTS1));
			
		    Set<TickerXTicker> alreadyStarted = new HashSet<TickerXTicker>();
		    
		    HashMap<String, HashMap<String, HashMap<TickerXTicker, Float>>> startYearBased = new HashMap<String, HashMap<String, HashMap<TickerXTicker, Float>>>();
		    
		    
		    AtomicInteger progress = new AtomicInteger(fdds.get("1980").size());
		    
		    int i = 0;
//		    for(i = 0; i < years.length; i++){
		    for(i = 0; i < 1; i++){
		    	
		    	String year = years[i];
		    	
//		    	PreparedStatement ps = conn.prepareStatement("select obf1,obf2,weightedTogetherness from PC_"+year+" order by weightedTogetherness desc limit 0,100");
		    	PreparedStatement ps = conn.prepareStatement("select obf1,obf2,weightedTogetherness from PC_"+year+" order by weightedTogetherness asc");
		    	ResultSet rs = ps.executeQuery();

		    	HashMap<String, HashMap<TickerXTicker, Float>> wal = new HashMap<String, HashMap<TickerXTicker, Float>>();
		    	startYearBased.put(year, wal);
		    	wal.put(year, new HashMap<TickerXTicker, Float>());
		    	
		    	while(rs.next()){
		    		
		    		System.out.println("combinations left: "+progress.decrementAndGet());
		    		
		    		try {

		    			TickerXTicker tt = new TickerXTicker(new Ticker(UUID.fromString(rs.getString(1))), new Ticker(UUID.fromString(rs.getString(2))));
		    			System.out.println(tt.toString()+" "+rs.getFloat(3));
		    			wal.get(year).put(tt, rs.getFloat(3));

		    			if(!alreadyStarted.contains(tt)){

		    				System.out.println("added "+year+" "+tt.toString()+" wt: "+rs.getFloat(3));

		    				if(i+1 < years.length){
		    					for(int j = i+1; j < years.length-i; j++){


		    							wal.computeIfAbsent(years[j], (g) -> new HashMap<TickerXTicker, Float>());

		    							Float val = 0f;
//		    							
		    							if(fdds.get(years[j]) != null){
		    								wal.get(years[j]).put(tt, fdds.get(years[j]).get(tt));
		    								val = fdds.get(years[j]).get(tt);
		    							}else{

				    						PreparedStatement ps2 = statements.get(years[j]);
				    						ps2.setString(1, rs.getString(1));
				    						ps2.setString(2, rs.getString(2));
		
				    						ResultSet rs2 = ps2.executeQuery();
		
				    						if(rs2.next()){

				    							wal.get(years[j]).put(tt, rs2.getFloat(3));
				    							val = rs2.getFloat(3);
				    						}
		    							}

		    							
		    							System.out.println("\tadded sub "+years[j]+" "+tt.toString()+" wt: "+val);
//		    						}else{
//		    							System.out.println("\tdidn't find "+years[j]+" "+tt.toString());
//		    						}
		    					}
		    				}
		    			}else{
		    				//System.out.println("already started (skipping) "+year+" "+tt.toString());
		    			}
		    			alreadyStarted.add(tt);
						
					} catch (NoTickerForUUIDException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    	}
		    }
		    
		    HashMap<String, HashMap<TickerXTicker, Float>> data = startYearBased.get("1980");
		    
		    HashMap<TickerXTicker, Float> d1 = data.get("1980");
		    
//		    Set<TickerXTicker> neverSeparated = new HashSet<TickerXTicker>(d1.keySet());
		    Set<TickerXTicker> separated = new HashSet<TickerXTicker>();
		    
		    
		    float max = 0f;
		    for(Entry<TickerXTicker, Float> dd : d1.entrySet()){
		    	TickerXTicker tt = dd.getKey();
		    	
		    	for(String year: years){		    		
		    		HashMap<TickerXTicker, Float> j = data.get(year);
		    		
		    		if(j != null){
		    			
		    			Float val = j.get(tt);
		    			if(val != null){
		    				max = Math.max(max, val);
		    
		    				if(val < 0){
		    					separated.add(tt);
		    				}
		    				
		    			}
		    			
		    		}
		    	}
		    }
		    
		    d1.keySet().removeAll(separated);
		    
		    max = Math.min(.8f, max);
		    System.out.println("max = "+max);
		    
		    
		    System.out.println("creating image...");
		    
		    BufferedImage bi = new BufferedImage(outImageW, outImageH, BufferedImage.TYPE_INT_RGB);
		    Graphics2D g2d = bi.createGraphics();
		    
		    g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		    g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
		    
		    int h = bi.getHeight();
		    int w = bi.getWidth();
		    
		    g2d.setBackground(new Color(244,244,244));
		    g2d.clearRect(0, 0,bi.getWidth(), bi.getHeight());
		    
		    
		    
		    g2d.setColor(Color.GRAY);
		    g2d.setStroke(new BasicStroke(1f));
		    for(int k = 1; k < Math.round(10); k++){
		    	int y = (h/2)+(((h/2)/10)*k);
		    	g2d.drawLine(0, y, bi.getWidth(), y);
		    	g2d.drawString(""+k, 20, y);
		    }
		    
		    g2d.setColor(Color.GREEN);
		    for(int k = 1; k < Math.round(10); k++){
		    	int y = (h/2)-(((h/2)/10)*k);		    	
		    	g2d.drawLine(0, y, bi.getWidth(), y);	
		    	g2d.drawString(""+k, 20, y);
		    }
		    
		    g2d.setStroke(new BasicStroke(1f));
		    
		    for(Entry<TickerXTicker, Float> dd : d1.entrySet()){
		    	TickerXTicker tt = dd.getKey();

		    	int lastX = 0;
		    	int lastY = 0;
		    	

		    	int R = (int) (Math.random( )*256);
		    	int G = (int)(Math.random( )*256);
		    	int B= (int)(Math.random( )*256);
		    	g2d.setColor(new Color(R, G, B));

		    	for(String year: years){
		    		
		    		HashMap<TickerXTicker, Float> j = data.get(year);
		    		
		    		if(j != null){
		    			
		    			Float val = j.get(tt);
		    			
		    			if(val == null){
		    				Color oldC = g2d.getColor();
		    				Stroke oldS = g2d.getStroke();
		    				
		    				g2d.setColor(Color.RED);
		    				g2d.setStroke(new BasicStroke(2f));
		    				
		    				g2d.drawLine(lastX, lastY, lastX, h);;
		    				
		    				g2d.setStroke(oldS);
		    				g2d.setColor(oldC);
		    				
		    			}else{


		    				float wi = (float)(Arrays.asList(years).indexOf(year)/(float)(years.length));

		    				//800 * 1.0  800 * .1
		    				//400 * .3

		    				//		    				try {
		    				//								if(tt.equals(new TickerXTicker(new Ticker("4908e4ce-506a-4894-8acf-7c7dc2678f04"),new Ticker(UUID.fromString("98e033c2-d42a-4857-a1f4-32720b34fe49"))))){
		    				//									int kkk = 0;
		    				//									System.out.println("kdkd");
		    				//								}
		    				//							} catch (NoTickerForUUIDException e1) {
		    				//								// TODO Auto-generated catch block
		    				//								e1.printStackTrace();
		    				//							}

		    				int destH = (h/2) - Math.round((h/2) * (val/max));
		    				int destW = Math.round(wi*w);


		    				if(lastX == 0 && lastY == 0){

		    				//	System.out.println("starting string for : "+tt.toString()+" "+year);
		    				}else{
		    					g2d.drawLine(lastX, lastY, destW, destH);
		    				//	System.out.println("string for : "+tt.toString()+" "+year+" w:"+destW+" h:"+destH+" val:"+val+" val/max"+(val/max)+" rounded: "+(Math.round((h/2) * (val/max))));
		    				}

		    				lastX = destW;
		    				lastY = destH;
		    			}

		    		}
		    	}
		    	
		    	g2d.setColor(Color.BLACK);
		    	g2d.setStroke(new BasicStroke(Math.max(1, 8*(h/10000))));
			    g2d.drawLine(0, bi.getHeight()/2, bi.getWidth(), bi.getHeight()/2);
			    
			    g2d.drawString("exhibited separation", bi.getWidth()/2, Math.round(bi.getHeight()*.8));
		    }

		    System.out.println("writing image...");
		    //		    int dpi = 100;
		    //		    try {
		    //				ImageIO.write(bi, "jpg", output);
		    //			} catch (IOException e1) {
		    //				// TODO Auto-generated catch block
		    //				e1.printStackTrace();
		    //			}

		    JPEGImageWriteParam jpegParams = new JPEGImageWriteParam(null);
		    jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
		    jpegParams.setCompressionQuality(1f);

		    final ImageWriter writer = ImageIO.getImageWritersByFormatName("jpg").next();
		    // specifies where the jpg image has to be written

		    FileImageOutputStream fios = null;
		    try {
		    	fios = new FileImageOutputStream(output);
		    	writer.setOutput(fios);
		    } catch (FileNotFoundException e2) {
		    	// TODO Auto-generated catch block
		    	e2.printStackTrace();
		    } catch (IOException e2) {
		    	// TODO Auto-generated catch block
		    	e2.printStackTrace();
		    }

		    // writes the file with given compression level 
		    // from your JPEGImageWriteParam instance
		    try {
		    	writer.write(null, new IIOImage(bi, null, null), jpegParams);
		    } catch (IOException e1) {
		    	// TODO Auto-generated catch block
		    	e1.printStackTrace();
		    }finally{
		    	try {
		    		fios.close();
		    	} catch (IOException e1) {
		    		// TODO Auto-generated catch block
		    		e1.printStackTrace();
		    	}
		    	
		    	writer.dispose();

		    }


		    System.out.println("done");
		    //			ImageSaver.saveJpeg(bi, bi.getWidth(), bi.getHeight(), dpi, output);

		} catch (SQLException ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
