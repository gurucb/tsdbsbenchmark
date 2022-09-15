package timeseries.benchmarktool.dataloader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.influxdb.InfluxDB;
import org.json.simple.JSONObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;



public class InfluxDataLoader implements DataLoader {
    public JSONObject json = new JSONObject();
    public InfluxDB influxDB;
    public int batchSize = 5;
    BlockingQueue < Object > bqueue;
    public String influxToken;
    public String bucketName;
    public String orgName;
    
    private static List<String> listFiles(final String directory) {
        if (directory == null) {
          return Collections.emptyList();
        }
        List<String> dataFilesList = new ArrayList<>();
        File[] files = new File(directory).listFiles();
        for (File element : files) {
          if (element.isDirectory()) {
            dataFilesList.addAll(listFiles(element.getPath()));
          } else {
            dataFilesList.add(element.getPath());
          }
        }
        return dataFilesList;
    }
    
    public InfluxDataLoader() {

    }
    private static final String getBasicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }
    
    public void uploadFile(String filePath) throws IOException {    		
    	OkHttpClient client = new OkHttpClient().newBuilder().build();
    			MediaType mediaType = MediaType.parse("text/plain");
    			StringBuilder contentBuilder = new StringBuilder();

    			try (Stream<String> stream 
    			  = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8))
    			{
    			  //Read the content with Stream
    			  stream.forEach(s -> contentBuilder.append(s).append("\n"));
    			}
    			catch (IOException e)
    			{
    			  e.printStackTrace();
    			}

    			String fileContent = contentBuilder.toString();
    			System.out.println(fileContent);
    			RequestBody body = RequestBody.create(mediaType,fileContent);
    			Request request = new Request.Builder()
    			  .url("http://localhost:8086/api/v2/write?org="+this.orgName+"&bucket="+this.bucketName+"&precision=ns")
    			  .method("POST", body)
    			  .addHeader("Authorization",this.influxToken)
    			  .addHeader("Content-Type", "text/plain")
    			  .build();
    			Response response = client.newCall(request).execute();
    			System.out.println(response);
    }
    
    @Override
    public void init(JSONObject config) {
        System.out.println("config is" + config);
        String sourceDirctory = (String) config.get("sourceDirectory");
        //int batchSize = (int)(long) config.get("batchSize");
        //int threadsCount = (int)(long) config.get("threadsCount");
        //boolean shouldDeleteSource = (boolean) config.get("shouldDeleteSource");
        JSONObject dbConfig = (JSONObject) config.get("dbConfig");
        JSONObject influxConfig = (JSONObject) dbConfig.get("influx");
        //String userName = (String) influxConfig.get("userName");
        //String password = (String) influxConfig.get("password");
        //boolean createDBIfNotExists = (boolean) influxConfig.get("createDBIfNotExists");        
        this.influxToken = (String) influxConfig.get("token");
        this.orgName = (String) influxConfig.get("org");
        this.bucketName = (String) influxConfig.get("bucketName");
       
        List<String> dataFilesList = listFiles(sourceDirctory);
        

        IntStream.range(0, dataFilesList.size()).parallel().forEach(index -> {
        	try {
				System.out.println(dataFilesList.get(index));
				this.uploadFile(dataFilesList.get(index));
				// Approach 1 [ Data truncate ]
				// MFileReader mFileReader = new MFileReader("C:\\tsbenchmarking\\data\\influx\\Avington\\7385C\\7385C.txt", 11111, 20);
				//        mFileReader.registerHanlder(new FileLineDataHandler());
				//        mFileReader.startRead();
				// Approach 2 [ read points in batch and insert] 
				//https://stackoverflow.com/questions/49811474/fastest-way-to-process-a-file-db-insert-java-multi-threading
				//        BalanceBatch balance = new BalanceBatch(5); 
				//        balance.startAll();
				//        try (Stream<String> stream = Files.lines(Paths.get("C:\\tsbenchmarking\\data\\influx\\Avington\\7385C1\\7385C.txt"))) {
				//            stream.forEach(balance::send);
				//        } catch (Exception e1) {
				//            e1.printStackTrace();
				//        }
				//        balance.stopAll();


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        });

    }
    @Override
    public void read() {

    }
    @Override
    public void write() {

    }
    @Override
    public void cleanup() {

    }
}


class DataReader implements Runnable {
    BlockingQueue < Object > blockingQueue;
    File file ;
    int rowProcessed;
    int batchSize;
    String filePath;

    DataReader(BlockingQueue<Object> blockingQueue, String filePath, int batchSize,int rowProcessed) {
        this.blockingQueue = blockingQueue;
        this.file = new File(filePath);
        this.rowProcessed = rowProcessed;
        this.batchSize = batchSize;
        this.filePath = filePath;
        
    }

    //	list all the files for the meter;
    //	for each file:
    //		500k
    //		 100k item at a time;

    @Override
    public void run() {
    	BufferedReader br = null;
        FileReader fr = null;
		List<String> buffer = new ArrayList<>();
		
		try (BufferedReader in = new BufferedReader(new FileReader(file))) {
		    String nextLine = null;
		    do  {
		        buffer.clear();
		        for (int i=0; i < this.batchSize; i++) {
		            // note that in.readLine() returns null at end-of-file
		            if ((nextLine = in.readLine()) == null) break;
		            this.blockingQueue.put(nextLine);
		        }
		    } while (nextLine != null);
		 } catch (IOException | InterruptedException ioe) {
		    // handle exceptions here
		 }
	
        		 
		try {
			int counter = 0;
			String line;
			fr = new FileReader(file); // java.io.FileReader
			br = new BufferedReader(fr); // java.io.BufferedReader
			while ((line = br.readLine()) != null && counter < batchSize) {
				this.blockingQueue.put(line);
				System.out.println("Adding item to the reader: "+line);
			}
			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
        
        // TODO Auto-generated method stub
        //		for (int i=0;i<5;i++) {
        //			try {
        //				this.blockingQueue.put(i);
        //				System.out.println("Adding item to the reader: "+i);
        //			} catch (InterruptedException e) {
        //				// TODO Auto-generated catch block
        //				e.printStackTrace();
        //			}
        //		}

    }
}

class DataWriter implements Runnable {
    BlockingQueue < Object > blockingQueue;
    int head = -1;
    DataWriter(BlockingQueue<Object> blockingQueue, String filePath, int batchSize,int rowProcessed) {
        this.blockingQueue = blockingQueue;
    };
    @Override
    public void run() {
        // TODO Auto-generated method stub
        while (head != 5) {
            try {
                head = (int) this.blockingQueue.take();
                System.out.print(head);
                System.out.println("Deleting item from the queue: " + head);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}