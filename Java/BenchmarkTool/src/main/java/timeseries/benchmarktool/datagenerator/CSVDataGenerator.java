package timeseries.benchmarktool.datagenerator;

import com.opencsv.CSVWriter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CSVDataGenerator implements DataGenerator {



    //	private String filePath;
//	private String outputDir;
//	private static final int NTHREDS = 10;
    private long parallelismFactor = 1;



    public CSVDataGenerator() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void generateData(String filePath, String outputDir) {
        // TODO Auto-generated method stub
        System.out.println("Timescale DB load generator");
//		this.filePath = filePath;
//		this.outputDir = outputDir;
        JSONParser parser = new JSONParser();
        try (FileReader reader = new FileReader(filePath)){
            Object obj = parser.parse(reader);
            JSONObject jsonObj = (JSONObject) obj;
            this.parallelismFactor = (Long) ((JSONObject) jsonObj.get("parallelism")).get("num_generators");

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }  catch (org.json.simple.parser.ParseException e) {
            e.printStackTrace();
        }



//		ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
//        for (int i = 0; i < this.parallelismFactor; i++) {
//            Runnable worker = new TimescaleLoadGenerator(filePath, outputDir);
//            executor.execute(worker);
//        }

        Runnable runnable = new CSVDataGenerator.CSVDataGeneratorThread(filePath, outputDir); // or an anonymous class, or lambda...
        for (int i = 0; i < this.parallelismFactor; i++) {
            Thread thread = new Thread(runnable);
            thread.start();
        }


    }


    class CSVDataGeneratorThread  implements Runnable {

        private String filePath;
        private String outputDir;

        public CSVDataGeneratorThread(String filePath, String outputDir) {
            this.filePath = filePath;
            this.outputDir = outputDir;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            System.out.println("TimescaleLG Thread  running : " + Thread.currentThread().getId());

            try {
                writeBuffered();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        private  void writeBuffered() throws IOException {

            File tmpDirectory = new File(this.outputDir + "\\");

            File file = File.createTempFile("timescale_load_", ".csv", tmpDirectory);
            List<String[]> csvData = createCsvData();

            try (CSVWriter writer = new CSVWriter(new FileWriter(file))) {
                writer.writeAll(csvData);
            }
        }

        private  List<String[]> createCsvData() {
            List<String> headers = new ArrayList<>();
            JSONParser parser = new JSONParser();
            JSONObject jsonSchemaObj = null;
            try (FileReader reader = new FileReader(filePath)){
                Object obj = parser.parse(reader);
                JSONObject jsonObj = (JSONObject) obj;
                JSONArray loadDetails = (JSONArray) jsonObj.get("load_details");
                //TODO
                // only using first schema for iteration 1
                String schemaFilePath = (String) ((JSONObject)loadDetails.get(0)).get("paylaod_json_scehma_list");
                try (FileReader schemaReader = new FileReader(schemaFilePath)) {
                    Object schemaObj = parser.parse(schemaReader);
                    jsonSchemaObj = (JSONObject) schemaObj;
                    Set keys = jsonSchemaObj.keySet();
                    keys.forEach(key -> {
                        headers.add((String) key);
                    });
                }
            }catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }  catch (org.json.simple.parser.ParseException e) {
                e.printStackTrace();
            }

            List<String[]> list = new ArrayList<>();
            String[] colNames = new String[headers.size()];
            colNames = headers.toArray(colNames);

            list.add(colNames);
            for(int i=0; i<5000; i++) {
                String [] record = generateRecord(jsonSchemaObj, headers);
                list.add(record);
            }
            return list;
        }

        private String[] generateRecord(JSONObject jsonSchemaObj, List<String> headers) {

            String[] record1 = {"1", "first name", "address 1", "11111"};
            return record1;
        }


    }

}
