package timeseries.benchmarktool.dataloader;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

class BatchInsert implements AutoCloseable {

    private int batchSize = 0;
    private final int batchLimit;
    InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "bhunesh", "Microsoft@123");

    
    public BatchInsert(int batchLimit) {
        this.batchLimit = batchLimit;
    }

    public void insert(String s) {
        Pong response = this.influxDB.ping();
        if (response.getVersion().equalsIgnoreCase("unknown")) {
            System.out.println("Error pinging server.");
            return;
        } else {
        	System.out.println("Success");
        }
    	System.out.println(s);
        if (++batchSize >= batchLimit) {
            sendBatch();
        }
    }

    public void sendBatch() {
        System.out.format("Send batch with %d records%n", batchSize);
        batchSize = 0;
    }

    @Override
    public void close() {
        if (batchSize != 0) {
            sendBatch();
        }
    }
}