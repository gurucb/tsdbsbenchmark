package timeseries.benchmarktool.datagenerator;

import com.opencsv.CSVWriter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class CSVDataGenerator implements DataGenerator {



    //	private String filePath;
//	private String outputDir;
//	private static final int NTHREDS = 10;
//    private long parallelismFactor = 1;



    public CSVDataGenerator() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void generateData(String configFilePath) {
        // TODO Auto-generated method stub
        System.out.println("CSV format data generator");

        try (FileReader reader = new FileReader(configFilePath)) {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(reader);
            JSONObject jsonObj = (JSONObject) obj;
            JSONArray schemaDetails= (JSONArray) jsonObj.get("schema_details");
            for(int i=0; i<schemaDetails.size(); i++) {
                createDataFilesFromSchema((JSONObject) schemaDetails.get(i));
            }

        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void createDataFilesFromSchema(JSONObject schemaDetail) throws IOException {
      
        //Number of threads running parallel to create data for X no. of sensor
        long numOfLocations = (long) schemaDetail.get("number_of_locations");
        String predefinedValuesFile = (String) schemaDetail.get("location_list");
        try (FileReader reader = new FileReader(predefinedValuesFile)) {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(reader);
            JSONObject jsonObj = (JSONObject) obj;
            JSONArray locations = (JSONArray) jsonObj.get("predefined_mappings");
            for (int i = 0; i < numOfLocations; i++) {
                JSONObject location = (JSONObject) locations.get(i);
                String city = (String) location.get("city");
                JSONArray coords =  (JSONArray) location.get("location");
                String locationStr = "[" + Double.toString((Double) coords.get(0)) +","+ Double.toString((Double) coords.get(1)) +"]";
                Runnable runnable = new CSVDataGenerator.CSVDataGeneratorThread(schemaDetail, city ,locationStr );
                Thread thread = new Thread(runnable);
                thread.start();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }



    }


    class CSVDataGeneratorThread  implements Runnable {

        private JSONObject schemaDetail;
        private String city;
        private String location;

        public CSVDataGeneratorThread(JSONObject schemaDetail, String city, String location) {
            this.schemaDetail = schemaDetail;
            this.city = city;
            this.location = location;
        }

        @Override
        public void run() {
            System.out.println("CSV Data generator Thread  running : " + Thread.currentThread().getId());
            try {
                createCsvFile(schemaDetail);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void createCsvFile(JSONObject schemaDetail) throws IOException {
            //location of directory where data files for a given schema will be stored
            //TODO directory should be already created
            File tmpDirectory = new File((String) schemaDetail.get("data_file_output_dir"));
            // file name for each data file
            File file = File.createTempFile((String) schemaDetail.get("file_name_prefix") + "_", ".csv", tmpDirectory);
            
            List<String[]> csvData = createCsvData(schemaDetail);
            try (CSVWriter writer = new CSVWriter(new FileWriter(file))) {
                writer.writeAll(csvData);
            }

        }

        private List<String[]> createCsvData(JSONObject schemaDetail) {
            JSONParser parser = new JSONParser();
            JSONObject jsonSchemaObj = null;
            List<String> headers = new ArrayList<>();
            String schemaFilePath = (String) schemaDetail.get("schema");
            try (FileReader schemaReader = new FileReader(schemaFilePath)) {
                Object schemaObj = parser.parse(schemaReader);
                jsonSchemaObj = (JSONObject) schemaObj;
                Set keys = jsonSchemaObj.keySet();
                keys.forEach(key -> {
                    headers.add((String) key);
                });
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date dateS = sdf.parse((String) schemaDetail.get("start_date"));
                Date dateE = sdf.parse((String) schemaDetail.get("end_date"));
                long msStart = dateS.getTime();
                long msEnd = dateE.getTime();
                long totalMilliseconds = msEnd - msStart;
                long interval = (long) schemaDetail.get("interval");
                String intervalUnit = (String) schemaDetail.get("interval_unit");
                long totalRecordsPerSensor = calculateTotalRecordsPerSensor(interval, intervalUnit, totalMilliseconds);
                System.err.println("Total Records for a schema " + totalRecordsPerSensor);
                List<String[]> list = new ArrayList<>();
                String[] colNames = new String[headers.size()];
                colNames = headers.toArray(colNames);
                list.add(colNames);

                for(int i=0; i<totalRecordsPerSensor; i++) {
                    String [] record = generateRecord(jsonSchemaObj, headers, msStart,  interval, intervalUnit, i );
                    list.add(record);
                }
                return list;


            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            } catch (java.text.ParseException e) {
                throw new RuntimeException(e);
            }

        }

        private long calculateTotalRecordsPerSensor(long interval, String intervalUnit, long totalMilliseconds) {
            switch(intervalUnit) {
                case "Nanosecond":
                    return (totalMilliseconds * 1000000)/interval;
                case "Microsecond":
                    return (totalMilliseconds * 1000)/interval;
                case "Millisecond":
                    return (totalMilliseconds * 1)/interval;
                case "Second":
                    return (totalMilliseconds / 1000)/interval;
                case "Minute":
                    return (totalMilliseconds / 60000)/interval;
                case "Hour":
                    return (totalMilliseconds / (60000*60))/interval;
                case "Day":
                    return (totalMilliseconds / ((60000*60*24)))/interval;
                default:
                    throw new RuntimeException("Interval Unit Not Supported");
            }
        }

        private String[] generateRecord(JSONObject jsonSchemaObj, List<String> headers, long msStart, long interval, String intervalUnit, int iteration) {
//            String[] record1 = {"1", "first name", "address 1", "11111"};

            List<String> record = new ArrayList<>();
            HashMap<String, String> parentValues = new HashMap<>();
            String[] recordStrArray = new String[headers.size()];
            for( String header: headers) {
                String data;
                JSONObject details = (JSONObject) jsonSchemaObj.get(header);
//                System.out.println(header + " :: " + details.get("type"));
                String type = (String) details.get("type");
                if( type.equals("timestamp")) {
                    String precision = (String) details.get("precision");
                    data = getTimestampVal(msStart, interval, intervalUnit, iteration, precision);
                } else if(header.equals("city")) {
                    data = city;
                } else if(header.equals("city")) {
                    data = location;
                }else if(header.equals("meter_id")) {
                    data = "MID-001";
                } else {
                    data = getColumnValue(header, details, parentValues);
                }
                record.add(data);
            }
            return record.toArray(recordStrArray);
        }

        private String getTimestampVal(long msStart, long interval, String intervalUnit, int iteration, String precision) {
            String randomTimeStamp;
            Random r = new Random();
            int incrementInIteration = (int) (interval * iteration);
            switch(precision) {
                case "Nanosecond" :
                    randomTimeStamp = String.valueOf((r.nextInt(999999-100000) + 100000));
                    break;
                case "Microsecond" :
                    randomTimeStamp = String.valueOf(r.nextInt(999-100) + 100);
                    break;
                case "Millisecond" :
                    randomTimeStamp =  "";
                    break;
//                case "Second" :
//                case "Minute" :
//                case "Hour" :
//                case "Day" :
                default:
                    randomTimeStamp = "";
            }
            switch(intervalUnit) {
//                case "Nanosecond" :
//                    if(incrementInIteration>=100 && incrementInIteration<=999) {
//                        randomTimeStamp = "000" + String.valueOf(incrementInIteration);
//                    } else if(incrementInIteration>=10 && incrementInIteration<=99) {
//                        randomTimeStamp = "0000" + String.valueOf(incrementInIteration);
//                    } else if(incrementInIteration>=1 && incrementInIteration<=9) {
//                        randomTimeStamp = "00000" + String.valueOf(incrementInIteration);
//                    } else {
//                        throw new RuntimeException("The interval value "+ interval + "is invalid for interval Unit " + intervalUnit);
//                    }
//                    return String.valueOf(msStart + randomTimeStamp);
//                case "Microsecond" :
//                    if(incrementInIteration>=100 && incrementInIteration<=999) {
//                        randomTimeStamp = String.valueOf(incrementInIteration);
//                    } else if(incrementInIteration>=10 && incrementInIteration<=99) {
//                        randomTimeStamp = "0" + String.valueOf(incrementInIteration);
//                    } else if(incrementInIteration>=1 && incrementInIteration<=9) {
//                        randomTimeStamp = "00" + String.valueOf(incrementInIteration);
//                    } else {
//                        throw new RuntimeException("The interval value "+ interval + "is invalid for interval Unit " + intervalUnit);
//                    }
//                    return String.valueOf(msStart + randomTimeStamp);
//                case "Millisecond" :
//                    return String.valueOf((msStart + incrementInIteration) + randomTimeStamp);
                case "Second" :
                    return String.valueOf(msStart + (incrementInIteration*1000) + randomTimeStamp) ;
//                case "Minute" :
//                    return String.valueOf(msStart + (incrementInIteration*1000*60) + randomTimeStamp) ;
//                case "Hour" :
//                    return String.valueOf(msStart + (incrementInIteration*1000*60*60) + randomTimeStamp) ;
//                case "Day" :
//                    return String.valueOf(msStart + (incrementInIteration*1000*60*60*24) + randomTimeStamp) ;
                default:
                    throw new RuntimeException("The interval unit "+ intervalUnit + " is invalid or not supported");
            }
        }

        private String getColumnValue(String header, JSONObject details ,  HashMap<String, String> parentValues) {

            String type = (String) details.get("type");
            Double min = (Double) details.get("min");
            Double max = (Double) details.get("max");

            //if column type is String (in most of the scenarios) and we need to have predefined values for that column
            boolean isPredefined = details.get("isPredefined")!=null ? (boolean) details.get("isPredefined") : false;
            String predefinedValuesFile = details.get("predefinedValuesFile")!=null ? (String) details.get("predefinedValuesFile") : "";

            //if column value depends on value of other column i.e. Parent column and by how much i.e. the fraction or percentage of the parent value
            boolean isChild = details.get("isChild")!=null ? (boolean) details.get("isChild") : false;
            String parentField =  details.get("parentField")!=null ? (String) details.get("parentField") : "";
            Double percent = details.get("percent")!=null ? (Double) details.get("percent") : 1;

            //if column has dependent columns i.e. child columns. So that such column value is stored for calculating the child col values.
            boolean hasDependents =  details.get("hasDependents")!=null ? (boolean) details.get("hasDependents") : false;

            Random randomNum = new Random();
            switch(type) {
                case "float":
                    Double floatValue;
                    if(!isPredefined) {
                        if(isChild) {
                            float parentVal = Float.parseFloat(parentValues.get(parentField));
                            floatValue = parentVal * percent;

                        } else {
                            floatValue = (randomNum.nextFloat()*(max - min) + min);
                            if(hasDependents) {
                                parentValues.put(header, String.valueOf(floatValue));
                            }
                        }
                        return String.valueOf(floatValue);
                    }
//                case "int":
//                    int intValue;
//                    if(!isPredefined) {
//                        if(isChild) {
//                            float parentVal = Float.parseFloat(parentValues.get(parentField));
//                            intValue = (int) (parentVal * percent);
//
//                        } else {
//                            intValue = (int) (randomNum.nextInt((int) (max - min))+ (int) min);
//                            if(hasDependents) {
//                                parentValues.put(header, String.valueOf(intValue));
//                            }
//                        }
//                        return String.valueOf(intValue);
//                    }
                case "String":
                    String stringValue;
                    if(isPredefined) {
                        if(header == "city") {
                            return this.city;
                        }
                        if(header == "location"){
                            return this.location;
                        }
                    }
                default:
                    //
                    return null;
            }
        }


    }

}
