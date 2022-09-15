package timeseries.benchmarktool.dataloader;

public class DataLoaderFactory {
	public DataLoader createDataLoader(String dbType) {
		if (dbType == null || dbType.isEmpty())
			throw new IllegalArgumentException("DB Type is a required argument");
	    switch (dbType) {
	    case "Influx":
	        return new InfluxDataLoader();
	    default:
	        throw new IllegalArgumentException("Unknown DB "+ dbType);
	    }
	}
}


