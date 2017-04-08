package com.liminala.hiris.gui;

import java.awt.EventQueue;
import java.awt.Font;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.SpringLayout;
import javax.swing.plaf.multi.MultiSplitPaneUI;

import com.liminala.hiris.NoTickerForUUIDException;
import com.liminala.hiris.Tick;
import com.liminala.hiris.Ticker;
import com.liminala.hiris.TickerXTicker;
import com.liminala.hiris.gui.XYLines.Notify;

import java.awt.Color;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.jdesktop.swingx.JXMultiSplitPane;
import org.jdesktop.swingx.MultiSplitLayout;
import org.jdesktop.swingx.MultiSplitLayout.Divider;
import org.jdesktop.swingx.MultiSplitLayout.Leaf;
import org.jdesktop.swingx.MultiSplitLayout.Split;

public class DataViewer {

	private JFrame frame;
	private XYLines xyLines;
	private XYLines xyLines2;
	private XYLines xyLines3;
	private XYLines xyLines4;
	
	private static ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat>(){

		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd");
		}
	};
	
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					DataViewer window = new DataViewer();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}

	public DataViewer (){
		
		initialize();
	}
	
	ThreadPoolExecutor exec = null;
	
	public void initialize() {
		
		exec = new ThreadPoolExecutor(1, 2, 0L, TimeUnit.DAYS, new ArrayBlockingQueue<Runnable>(4));
		
		frame = new JFrame();
		
		frame.setBounds(10, 10, 800, 602);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		SpringLayout springLayout = new SpringLayout();
		frame.getContentPane().setLayout(springLayout);
		frame.setResizable(true);
		
		JLabel lblHeader = new JLabel("Hiris");
		lblHeader.setFont(new Font("Lucida Grande", Font.BOLD, 15));
		frame.getContentPane().add(lblHeader);

		JButton btnRepaint = new JButton("repaint");
		springLayout.putConstraint(SpringLayout.NORTH, btnRepaint, 2, SpringLayout.NORTH, frame.getContentPane());
		springLayout.putConstraint(SpringLayout.WEST, btnRepaint, 2, SpringLayout.EAST, lblHeader);
		frame.getContentPane().add(btnRepaint);
		
		btnRepaint.addActionListener(l -> xyLines.repaint());
		
		JPanel graphs = new JPanel();
		springLayout.putConstraint(SpringLayout.NORTH, graphs, 1, SpringLayout.SOUTH, lblHeader);
		springLayout.putConstraint(SpringLayout.WEST, graphs, 10, SpringLayout.WEST, frame.getContentPane());
		springLayout.putConstraint(SpringLayout.SOUTH, graphs, -10, SpringLayout.SOUTH, frame.getContentPane());
		springLayout.putConstraint(SpringLayout.EAST, graphs, 10, SpringLayout.EAST, frame.getContentPane());
		graphs.setBackground(Color.WHITE);
		frame.getContentPane().add(graphs);
		
		BoxLayout bl = new BoxLayout(graphs, BoxLayout.Y_AXIS);
		
		graphs.setLayout(bl);
		
		
		Split modelRoot = new Split(); 
		modelRoot.setRowLayout(false);
		
		Leaf first = new Leaf("first");
		first.setWeight(.25f);
		Leaf second = new Leaf("second");
		second.setWeight(.25f);
		Leaf third = new Leaf("third");
		third.setWeight(.25f);
		Leaf forth = new Leaf("forth");
		forth.setWeight(.25f);
		
		List children = Arrays.asList(first, new Divider(), second, new Divider(), third, new Divider(), forth); 
		
		modelRoot.setChildren(children);
		
		JXMultiSplitPane multiSplitPane = new JXMultiSplitPane();
		multiSplitPane.getMultiSplitLayout().setModel(modelRoot); 
		graphs.add(multiSplitPane);
		
		
		xyLines = new XYLines();
//		springLayout.putConstraint(SpringLayout.NORTH, xyLines, 1, SpringLayout.SOUTH, lblHeader);
//		springLayout.putConstraint(SpringLayout.WEST, xyLines, 10, SpringLayout.WEST, frame.getContentPane());
//		springLayout.putConstraint(SpringLayout.SOUTH, xyLines, ((frame.getHeight()/4)), SpringLayout.SOUTH, lblHeader);
//		springLayout.putConstraint(SpringLayout.EAST, xyLines, 20, SpringLayout.EAST, frame.getContentPane());
		xyLines.setBackground(Color.WHITE);
//		graphs.add(xyLines);
		multiSplitPane.add(xyLines, "first"); 


		
		xyLines.setNotify(new Notify(){

			@Override
			public void onClick(Object o, Date first, Date last){
				
				Runnable r = new Runnable(){

					@Override
					public void run() {
						
						System.out.println("getting percent changes from DB for "+o);
						TreeMap<Date, HashMap<Tick, Float>> x = getPercentChangeData((TickerXTicker)o, first, last);
						
						
						float max = 0f;
						float min = Float.MAX_VALUE;
						for(HashMap<Tick, Float> dd : x.values()){
							for(Float pp : dd.values()){
								max = Math.max(max, pp);
								min = Math.min(min, pp);
							}
						}
						
						xyLines2.setMin(min);
						xyLines2.setMax(max);
						xyLines2.getData().clear();
						xyLines2.getData().putAll(x);
						xyLines2.setNumBuckets(x.size());
						xyLines2.setRange0to1(false);
						
						
						System.out.println("percent changes set for "+o);
						EventQueue.invokeLater(new Runnable() {
							public void run() {
								try {
									xyLines2.repaint();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
						
						//--------------------
						
						TreeMap<Date, HashMap<Tick, Float>> avgs = makeRollingAverages(x, 20);
						
						float maxAvg = 0;
						float minAvg = Float.MAX_VALUE;
						for(HashMap<Tick, Float> dd : avgs.values()){
							for(Float pp : dd.values()){
								maxAvg = Math.max(maxAvg, pp);
								minAvg = Math.min(minAvg, pp);
							}
						}
						
						xyLines3.setMin(minAvg);
						xyLines3.setMax(maxAvg);
						xyLines3.getData().clear();
						xyLines3.getData().putAll(avgs);
						xyLines3.setNumBuckets(avgs.size());
						xyLines3.setRange0to1(false);
						
						System.out.println("avg percent changes set for "+o);
						EventQueue.invokeLater(new Runnable() {
							public void run() {
								try {
									xyLines3.repaint();			
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
						
						//-------------------
						TreeMap<Date, HashMap<Tick, Float>> xx = getAdjClose((TickerXTicker)o, first, last);
						
						TreeMap<Date, HashMap<Tick, Float>> avgs2 = makeRollingAverages(xx, 5);
						
						float maxAvg2 = 0;
						float minAvg2 = Float.MAX_VALUE;
						for(HashMap<Tick, Float> dd : avgs2.values()){
							for(Float pp : dd.values()){
								maxAvg2 = Math.max(maxAvg2, pp);
								minAvg2 = Math.min(minAvg2, pp);
							}
						}
						
						xyLines4.setMax(minAvg2);
						xyLines4.setMax(maxAvg2);
						xyLines4.getData().clear();
						xyLines4.getData().putAll(avgs2);
						xyLines4.setNumBuckets(avgs2.size());
						xyLines4.setRange0to1(false);
						
						System.out.println("avg close changes set for "+o);
						EventQueue.invokeLater(new Runnable() {
							public void run() {
								try {
									xyLines4.repaint();			
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
						
					}
					
				};
				exec.execute(r);
				
				
			}});
		
		
		xyLines2 = new XYLines();
//		springLayout.putConstraint(SpringLayout.NORTH, xyLines2, 1, SpringLayout.SOUTH, xyLines);
//		springLayout.putConstraint(SpringLayout.WEST, xyLines2, 10, SpringLayout.WEST, frame.getContentPane());
//		springLayout.putConstraint(SpringLayout.SOUTH, xyLines2, ((frame.getHeight()/4)), SpringLayout.SOUTH, xyLines);
//		springLayout.putConstraint(SpringLayout.EAST, xyLines2, 20, SpringLayout.EAST, frame.getContentPane());
		xyLines2.setBackground(Color.WHITE);
//		graphs.add(xyLines2);
		multiSplitPane.add(xyLines2, "second");
		xyLines2.setCompareTwoPanel(true);
		
		
		xyLines3 = new XYLines();
//		springLayout.putConstraint(SpringLayout.NORTH, xyLines3, 1, SpringLayout.SOUTH, xyLines2);
//		springLayout.putConstraint(SpringLayout.WEST, xyLines3, 10, SpringLayout.WEST, frame.getContentPane());
//		springLayout.putConstraint(SpringLayout.SOUTH, xyLines3,  ((frame.getHeight()/4)), SpringLayout.SOUTH, xyLines2);
//		springLayout.putConstraint(SpringLayout.EAST, xyLines3, 20, SpringLayout.EAST, frame.getContentPane());
		xyLines3.setBackground(Color.WHITE);
//		graphs.add(xyLines3);
		multiSplitPane.add(xyLines3, "third");
		xyLines3.setCompareTwoPanel(true);
		
		xyLines4 = new XYLines();
//		springLayout.putConstraint(SpringLayout.NORTH, xyLines4, 1, SpringLayout.SOUTH, xyLines3);
//		springLayout.putConstraint(SpringLayout.WEST, xyLines4, 10, SpringLayout.WEST, frame.getContentPane());
//		springLayout.putConstraint(SpringLayout.SOUTH, xyLines4,  ((frame.getHeight()/4)), SpringLayout.SOUTH, xyLines3);
//		springLayout.putConstraint(SpringLayout.EAST, xyLines4, 20, SpringLayout.EAST, frame.getContentPane());
		xyLines4.setBackground(Color.WHITE);
//		graphs.add(xyLines4);
		multiSplitPane.add(xyLines4, "forth");
		xyLines4.setCompareTwoPanel(true);
		xyLines4.setOnlyPositive(true);
	}
	
	protected TreeMap<Date, HashMap<Tick, Float>> makeRollingAverages(TreeMap<Date, HashMap<Tick, Float>> x, int days) {
		
		TreeMap<Date, HashMap<Tick, Float>> ret = new TreeMap<Date, HashMap<Tick, Float>>();
		
		HashMap<Tick, LinkedBlockingQueue<Float>> past = new HashMap<>();
		int dayCount = 1;
		float sum = 0;
		int count = 1;
		
		for(Entry<Date, HashMap<Tick, Float>> ent : x.entrySet()){
			
			for(Entry<Tick, Float> e : ent.getValue().entrySet()){
				
				LinkedBlockingQueue<Float> fifo = past.computeIfAbsent(e.getKey(), y -> new LinkedBlockingQueue<Float>(days));
				
				if(fifo.size() >= days){
					fifo.poll();
//					System.out.println("popping fifo  "+df.get().format(ent.getKey())+" for "+e.getKey());
				}
				
				sum = e.getValue();
				count = 1;
				for(Float f : fifo){
					sum += f;
//					System.out.printf("\t%s %s days: %d val %f fifoval %f sum %f rAvg: %f fifo size: %d\n",df.get().format(ent.getKey()), e.getKey(), count, e.getValue(), f, sum, sum/count, fifo.size());
					count++;
				}
				
				fifo.offer(e.getValue());
//				System.out.println("sticking value in queue: "+e.getValue()+" fifo size: "+fifo.size()+" "+e.getKey());
				
//				System.out.printf("%s %s days: %d sum %f rAvg: %f fifo size:\n",df.get().format(ent.getKey()), e.getKey(), count, sum, sum/count, fifo.size());
				
				if(dayCount % days == 0){
					ret.computeIfAbsent(ent.getKey(), h -> new HashMap<Tick, Float>()).put(e.getKey(), new Float(sum/count));
//					System.out.printf("%s %s days: %d sum %f rAvg: %f fifo size: %d\n",df.get().format(ent.getKey()), e.getKey(), count, sum, sum/count, fifo.size());
				}	
				
			}
			dayCount++;
		}

		return ret;
	}
	
	protected TreeMap<Date, HashMap<Tick, Float>> getPercentChangeData(TickerXTicker o, Date start, Date end) {
		
		TreeMap<Date, HashMap<Tick, Float>> ret = new TreeMap<Date, HashMap<Tick, Float>>();
		
		Connection con = null;
		try {
			
			con = DriverManager.getConnection("jdbc:mysql://192.168.3.115:3306/hiris?user=m&password=m");
			
			PreparedStatement stmt = con.prepareStatement("select date, obf, percentChange from hiris.PercentChange1Day where obf = ? and date >= ? and date <= ? order by date asc");
			
			stmt.setString(1, o.getT1().getId().toString());
			stmt.setString(2, df.get().format(start));
			stmt.setString(3, df.get().format(end));
			
			ResultSet rs = stmt.executeQuery();
			TreeMap<Date, HashMap<Tick, Float>> retQuery1 = new TreeMap<Date, HashMap<Tick, Float>>();
			while(rs.next()){
				try {
					retQuery1.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("percentChange"));
					ret.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("percentChange"));
				} catch (NoTickerForUUIDException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			rs = null;
			
			System.out.println("selected 1 day percent changes for: "+o.getT1().getId().toString()+", count = "+retQuery1.size()+" using date range: "+df.get().format(start)+"/"+df.get().format(end) );
			
			stmt.setString(1, o.getT2().getId().toString());
			stmt.setString(2, df.get().format(start));
			stmt.setString(3, df.get().format(end));
			
			rs = stmt.executeQuery();
			
			TreeMap<Date, HashMap<Tick, Float>> retQuery2 = new TreeMap<Date, HashMap<Tick, Float>>();
			while(rs.next()){
				try {
					retQuery2.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("percentChange"));
					ret.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("percentChange"));
				} catch (NoTickerForUUIDException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("selected 1 day percent changes for: "+o.getT2().getId().toString()+", count = "+retQuery2.size()+" using date range: "+df.get().format(start)+"/"+df.get().format(end) );
		
			
			System.out.println("total dates returned: "+ret.size());
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		return ret;
		
	}

	
protected TreeMap<Date, HashMap<Tick, Float>> getAdjClose(TickerXTicker o, Date start, Date end) {
		
		TreeMap<Date, HashMap<Tick, Float>> ret = new TreeMap<Date, HashMap<Tick, Float>>();
		
		Connection con = null;
		try {
			
			con = DriverManager.getConnection("jdbc:mysql://192.168.3.115:3306/hiris?user=m&password=m");
			
			PreparedStatement stmt = con.prepareStatement("select date, obf, adjClose from hiris.EOD where obf = ? and date >= ? and date <= ? order by date asc");
			
			stmt.setString(1, o.getT1().getId().toString());
			stmt.setString(2, df.get().format(start));
			stmt.setString(3, df.get().format(end));
			
			ResultSet rs = stmt.executeQuery();
			TreeMap<Date, HashMap<Tick, Float>> retQuery1 = new TreeMap<Date, HashMap<Tick, Float>>();
			while(rs.next()){
				try {
					retQuery1.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("adjClose"));
					ret.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("adjClose"));
				} catch (NoTickerForUUIDException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			rs = null;
			
			System.out.println("selected adjClose for: "+o.getT1().getId().toString()+", count = "+retQuery1.size()+" using date range: "+df.get().format(start)+"/"+df.get().format(end) );
			
			stmt.setString(1, o.getT2().getId().toString());
			stmt.setString(2, df.get().format(start));
			stmt.setString(3, df.get().format(end));
			
			rs = stmt.executeQuery();
			
			TreeMap<Date, HashMap<Tick, Float>> retQuery2 = new TreeMap<Date, HashMap<Tick, Float>>();
			while(rs.next()){
				try {
					retQuery2.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("adjClose"));
					ret.computeIfAbsent(new Date(rs.getDate("date").getTime()), (k) -> new HashMap<Tick, Float>()).put(new Ticker(UUID.fromString(rs.getString("obf"))), rs.getFloat("adjClose"));
				} catch (NoTickerForUUIDException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("selected adjClose for: "+o.getT2().getId().toString()+", count = "+retQuery2.size()+" using date range: "+df.get().format(start)+"/"+df.get().format(end) );
		
			
			System.out.println("total dates returned: "+ret.size());
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		return ret;
		
	}
	public void repaintXYLines(){
		xyLines.repaint();
	}

	public JFrame getFrame() {
		return frame;
	}

	public XYLines getXyLines() {
		return xyLines;
	}

	public XYLines getXyLines2() {
		return xyLines2;
	}

	public XYLines getXyLines3() {
		return xyLines3;
	}

}
