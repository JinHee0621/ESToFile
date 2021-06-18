package gui;

import java.awt.Color;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingWorker;

import testSparkApp.elasticJoin;

class ChoosePath extends JFrame{
	public GraphicUi guiFrame;
	public JFileChooser chooser = new JFileChooser();
	
	public ChoosePath(GraphicUi guiFrame) {
		this.guiFrame = guiFrame;
		
		chooser.setCurrentDirectory(new File("C:\\"));
		chooser.setFileSelectionMode(chooser.DIRECTORIES_ONLY);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		int re = chooser.showSaveDialog(null);
		if(re == JFileChooser.APPROVE_OPTION) {
			guiFrame.filePath = chooser.getSelectedFile().getAbsolutePath();
			guiFrame.fileAddress.setText(guiFrame.filePath);
		}
		
	}
	
}

class AlertCon extends JFrame implements ActionListener{
	public GraphicUi mainFrame;
	
	JTextArea alertContent = new JTextArea(5,8);
	public JButton btn = new JButton("확인");
	
	Container subCon;
	
	Image icon = null;
	
	
	public AlertCon(String content) {
		try {
			icon = Toolkit.getDefaultToolkit().getImage(getClass().getResource("Resources/warning.png"));
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		icon = icon.getScaledInstance(45, 45, Image.SCALE_SMOOTH);
		
		alertContent.setBackground(null);
		alertContent.setEditable(false);
		
		setSize(375, 250);
		subCon = getContentPane();
		subCon.setLayout(new FlowLayout(FlowLayout.CENTER, 130, 10));
		alertContent.setText(content);
		btn.addActionListener(this);
		subCon.add(new JLabel(new ImageIcon(icon)));
		subCon.add(alertContent);
		subCon.add(btn);
		
		setResizable(false);
		setVisible(true);
		setLocationRelativeTo(null);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		dispose();
	}
}


public class GraphicUi extends JFrame implements ActionListener{
	public String filePath = "";
	public JLabel fileAddress = new JLabel("C:\\");
	
	JTextField host = new JTextField(35);
	public JButton b1 = new JButton("실행");
	public JButton b2 = new JButton("경로");
	
	
	JTextArea query = new JTextArea(10,35);
	Image title = null;
	
	public JProgressBar progress = new JProgressBar();
	public int progressVal = 0;
	
	public JLabel resultText = new JLabel("             ");
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
		
		setLayout(new FlowLayout(FlowLayout.CENTER, 140, 20));
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
		thisCon.add(b2);
		b2.addActionListener(this);
		
		thisCon.add(b1);
		b1.addActionListener(this);

		
		thisCon.add(fileAddress);
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
		if(e.getSource() == b1) {
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
		} else if(e.getSource() == b2) {
			new ChoosePath(this);
		}
	
	}
	
	public void callAlert(String content) {
		new AlertCon(content);
	}

}
