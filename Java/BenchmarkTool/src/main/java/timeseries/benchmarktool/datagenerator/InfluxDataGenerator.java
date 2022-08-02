/**
 * 
 */
package timeseries.benchmarktool.datagenerator;

/**
 * @author kishoripatil
 *
 */
public class InfluxDataGenerator implements DataGenerator, Runnable {

	@Override
	public void generateData(String filePath, String outputDir) {
		// TODO Auto-generated method stub
		System.out.println("Influx DB load generator");
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Thread running");
		
	}
}
