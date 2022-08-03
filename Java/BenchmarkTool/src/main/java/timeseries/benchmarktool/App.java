package timeseries.benchmarktool;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import timeseries.benchmarktool.datagenerator.DataGenerator;
import timeseries.benchmarktool.datagenerator.DataGeneratorFactory;

/**
 * BenchMark Tool
 * java appName jsonFile
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Starting BenchMark Tool" );
        
        String filePath = "C:\\Repos\\tsdbsbenchmark\\Java\\BenchmarkTool\\src\\resource\\config.json";

        JSONParser jsonParser = new JSONParser();
        String dbType = null;
        String dataGenerationJson = null;
        String loadDataOutputDir = null;
    	String queryOutputDir = null;
    	String connectionString = null;
    	String logsDir = null;
        String sampleCSVfilesDir = null;
    	boolean generateData = true;
        boolean transformData = true;
    	boolean loadData = true;
    	boolean generateQueries = true;
    	boolean executeQueries = true;

        try (FileReader reader = new FileReader(filePath))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONObject details = (JSONObject) obj;
            
            dbType = (String) details.get("dbType");
            dataGenerationJson = (String) details.get("data_generation_file_path");
            loadDataOutputDir = (String) details.get("load_data_output_dir");
            queryOutputDir = (String) details.get("query_output_dir");
            connectionString = (String) details.get("connection_string");
            generateData = (boolean) details.get("generateData");
            transformData = (boolean) details.get("transformData");
            loadData = (boolean) details.get("loadData");
            generateQueries = (boolean) details.get("generateQueries");
            executeQueries = (boolean) details.get("executeQueries");
            logsDir = (String) details.get("logs_dir");

            if(generateData) {
                DataGeneratorFactory dataGeneratorFactory = new DataGeneratorFactory();
//            DataGenerator dataGenerator = dataGeneratorFactory.createDataGenerator(dbType);
                DataGenerator dataGenerator = dataGeneratorFactory.createDataGeneratorByFormat("CSV");
                dataGenerator.generateData(dataGenerationJson);


            }
            if(transformData) {

            }

            if(loadData) {
                // ingest Data
            }

            if(generateQueries) {
                // generate Queries
            }

            if(executeQueries) {
                // execute Queries
            }
 
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        

    }
}
