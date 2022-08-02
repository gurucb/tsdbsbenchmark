/**
 * 
 */
package timeseries.benchmarktool.datagenerator;


/**
 * @author kishoripatil
 *
 */
public class CrateDataGenerator implements DataGenerator, Runnable {

	@Override
	public void generateData(String filePath, String outputDir) {
		// TODO Auto-generated method stub
		System.out.println("Crate DB load generator");
		run();

		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Thread running");
		
	}

}
