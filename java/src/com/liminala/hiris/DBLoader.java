package com.liminala.hiris;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.Set;

public class DBLoader {

	public static void main(String[] args) {
		
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
		HashMap<String, String> obfMap = CSVLoader3.getObfMap(obfMapFile);
		for(Entry<String, String> e : obfMap.entrySet()){
			Ticker.getUuidForTicker().put(e.getKey(), UUID.fromString(e.getValue()));
		}
		for(Entry<String, String> e : obfMap.entrySet()){
			Ticker.getTickerForUUID().put(UUID.fromString(e.getValue()), e.getKey());
		}
		
		
		
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

		    
		    String[] years = new String[]{
		    		"1980","1981","1982","1983","1984","1985","1986","1987","1988","1989",
		    		"1990","1991","1992","1993","1994","1995","1996","1997","1998","1999",
		    		"2000","2001","2002","2003","2004","2005","2006","2007","2008","2009",
		    		"2010","2011","2012","2013","2014","2015"};
		    
		    
		    HashMap<String, PreparedStatement> statements = new HashMap<>();
		    
		    for(String year : years){
		    	PreparedStatement ps = conn.prepareStatement("select obf1,obf2,weightedTogetherness from PC_"+year+" where obf1 = ? and obf2 = ?");
		    	statements.put(year, ps);
		    }
		    
		    Set<TickerXTicker> alreadyStarted = new HashSet<TickerXTicker>();
		    
		    HashMap<String, HashMap<String, HashMap<TickerXTicker, Float>>> startYearBased = new HashMap<String, HashMap<String, HashMap<TickerXTicker, Float>>>();
		    
		    int i = 0;
		    for(i = 0; i < years.length; i++){
		    	
		    	String year = years[i];
		    	
		    	PreparedStatement ps = conn.prepareStatement("select obf1,obf2,weightedTogetherness from PC_"+year+" order by weightedTogetherness desc limit 0,10");
		    	ResultSet rs = ps.executeQuery();

		    	HashMap<String, HashMap<TickerXTicker, Float>> wal = new HashMap<String, HashMap<TickerXTicker, Float>>();
		    	startYearBased.put(year, wal);
		    	wal.put(year, new HashMap<TickerXTicker, Float>());
		    	
		    	while(rs.next()){

		    		try {

		    			TickerXTicker tt = new TickerXTicker(new Ticker(UUID.fromString(rs.getString(1))), new Ticker(UUID.fromString(rs.getString(2))));
		    			System.out.println(tt.toString()+" "+rs.getFloat(3));
		    			wal.get(year).put(tt, rs.getFloat(3));

		    			if(!alreadyStarted.contains(tt)){

		    				System.out.println("added "+year+" "+tt.toString()+" wt: "+rs.getFloat(3));

		    				if(i+1 < years.length){
		    					for(int j = i+1; j < years.length-i; j++){

		    						PreparedStatement ps2 = statements.get(years[j]);
		    						ps2.setString(1, rs.getString(1));
		    						ps2.setString(2, rs.getString(2));

		    						ResultSet rs2 = ps2.executeQuery();

		    						if(rs2.next()){

		    							wal.computeIfAbsent(years[j], (g) -> new HashMap<TickerXTicker, Float>());

		    							wal.get(years[j]).put(tt, rs2.getFloat(3));

		    							System.out.println("\tadded sub "+years[j]+" "+tt.toString()+" wt: "+rs2.getFloat(3));
		    						}else{
		    							System.out.println("\tdidn't find "+years[j]+" "+tt.toString());
		    						}

		    					}
		    				}
		    			}else{
		    				System.out.println("already started (skipping) "+year+" "+tt.toString());
		    			}
		    			alreadyStarted.add(tt);
						
					} catch (NoTickerForUUIDException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    	}
		    }
		    
		    
		    
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
