import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class CustomReceiver extends Receiver<String> {

	public CustomReceiver(String line) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		
		
	}

	

	@Override
	public  void onStart() {
		
		new Thread(this::receive).start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub
		
	}
	
	public void receive() {
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader("src/main/resources/heroes_information.csv"));
			String line = null ;
			
			try {
				while((line = br.readLine()) != null) {
				//while((br.readLine()!=null)){
					
						
				
				//	System.out.println("IPL data set"+" "+line);
					
					store(line);
					
				//	System.out.println(br);
						
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("ex1"+e.getMessage());
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.out.println("called"+e.getMessage());
			e.printStackTrace();
		}
		
	}

}
