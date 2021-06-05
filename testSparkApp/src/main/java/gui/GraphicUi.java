package gui;

import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import testSparkApp.elasticJoin;

public class GraphicUi extends JFrame implements ActionListener{
	
	JTextField host = new JTextField(35);
	JButton b1 = new JButton("실행");
	JTextArea query = new JTextArea(10,35);
	Image title = null;
	
	public GraphicUi() {
		
		try {
			File titleImage = new File("Resources/SparkEstoFileTitle.png");
			title = ImageIO.read(titleImage);
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		setTitle("ESToFile");

		setResizable(false);

		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		setLayout(new FlowLayout(FlowLayout.CENTER, 80, 20));
		Container thisCon = getContentPane();
		
/*	    // 프레임(자바 화면) 크기
	    Dimension frameSize = getSize();
	    // 모니터 크기
	    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	    // (모니터화면 가로 - 프레임화면 가로) / 2, (모니터화면 세로 - 프레임화면 세로) / 2
	    setLocation((screenSize.width - frameSize.width) /2, (screenSize.height - frameSize.height) /2);
*/
		thisCon.add(new JLabel(new ImageIcon(title)));
		thisCon.add(new JLabel("Elastic Host"));
		thisCon.add(host);
		thisCon.add(new JLabel("Query"));
		thisCon.add(new JScrollPane(query));
		thisCon.add(b1);
		
		b1.addActionListener(this);
		
		setSize(450,550);
		setVisible(true);
	}
	public static void main(String[] args) {
		new GraphicUi();
	}
	
	@Override
	public void actionPerformed(ActionEvent e) {
		
		elasticJoin join = new elasticJoin();
	}

}
