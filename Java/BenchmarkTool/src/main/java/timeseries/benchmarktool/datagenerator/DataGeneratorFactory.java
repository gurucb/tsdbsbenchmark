/**
 * 
 */
package timeseries.benchmarktool.datagenerator;

/**
 * @author kishoripatil
 *
 */
public class DataGeneratorFactory {
	
//	public DataGenerator createDataGenerator(String dbType) {
//		if (dbType == null || dbType.isEmpty())
//			throw new IllegalArgumentException("DB Type is a required argument");
//        switch (dbType) {
//        case "Influx":
//            return new InfluxDataGenerator();
//        case "Timescale":
//            return new TimescaleDataGenerator();
//        case "Crate":
//            return new CrateDataGenerator();
//        default:
//            throw new IllegalArgumentException("Unknown DB "+ dbType);
//        }
//	}

    public DataGenerator createDataGeneratorByFormat(String format) {
        if (format == null || format.isEmpty())
            throw new IllegalArgumentException("Format is a required argument");
        switch (format) {
            case "CSV":
                return new CSVDataGenerator();
            default:
                throw new IllegalArgumentException("Unknown format "+ format);
        }
    }

}
