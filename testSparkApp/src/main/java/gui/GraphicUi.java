package gui;

import java.awt.Color;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingWorker;

import testSparkApp.elasticJoin;

public class GraphicUi extends JFrame implements ActionListener{
	
	JTextField host = new JTextField(35);
	public JButton b1 = new JButton("실행");
	JTextArea query = new JTextArea(10,35);
	Image title = null;
	
	public JProgressBar progress = new JProgressBar();
	public int progressVal = 0;
	
	public JLabel resultText = new JLabel();
	public boolean isRunning = false;
	
	public GraphicUi() {
		
		try {
			title = Toolkit.getDefaultToolkit().getImage(getClass().getResource("Resources/SparkEstoFileTitle.png"));
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		setTitle("ESToFile");

		setResizable(false);

		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		setLayout(new FlowLayout(FlowLayout.CENTER, 180, 20));
		Container thisCon = getContentPane();
		
/*	    // 프레임(자바 화면) 크기
	    Dimension frameSize = getSize();
	    // 모니터 크기
	    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	    // (모니터화면 가로 - 프레임화면 가로) / 2, (모니터화면 세로 - 프레임화면 세로) / 2
	    setLocation((screenSize.width - frameSize.width) /2, (screenSize.height - frameSize.height) /2);
*/
		
		b1.setText("실행");
		b1.setForeground(new Color(0,0,0));
		b1.setBackground(new Color(38,136,225));
		
		thisCon.add(new JLabel(new ImageIcon(title)));
		thisCon.add(new JLabel("Elastic Host"));
		thisCon.add(host);
		thisCon.add(new JLabel("Query"));
		thisCon.add(new JScrollPane(query));
		thisCon.add(b1);
		b1.addActionListener(this);
		
		thisCon.add(progress);
		thisCon.add(resultText);
		
		setSize(450,700);
		setVisible(true);
	}
	public static void main(String[] args) {
		new GraphicUi();
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		if(!isRunning) {
			isRunning = true;
			elasticJoin join = new elasticJoin(this);
			
			String data = host.getText() + "□" + query.getText();
			
	        final SwingWorker<Void, Void> worker = new SwingWorker<Void, Void>() {
	            @Override
	            protected Void doInBackground() throws Exception {
	            	join.runEsToFile(data);
	                //progress.setValue(0);
	                return null;
	            }
	        };
	        worker.execute();
		}
	}

}
