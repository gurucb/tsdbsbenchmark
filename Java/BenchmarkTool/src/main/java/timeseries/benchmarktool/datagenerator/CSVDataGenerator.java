package timeseries.benchmarktool.datagenerator;

import com.opencsv.CSVWriter;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class CSVDataGenerator implements DataGenerator {
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
            for (Object schemaDetail : schemaDetails) {
                createDataFilesFromSchema((JSONObject) schemaDetail);
            }

        } catch (ParseException | IOException e) {
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
                String locationStr = "[" +  coords.get(0) +","+ coords.get(1) +"]";
                Runnable runnable = new CSVDataGenerator.CSVDataGeneratorThread(schemaDetail, city ,locationStr );
                Thread thread = new Thread(runnable);
                thread.start();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }



    }


    class CSVDataGeneratorThread  implements Runnable {

        private final JSONObject schemaDetail;
        private final String city;
        private final String location;
        final double MEG = (Math.pow(1024, 2));
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        final SimpleDateFormat sdfFileName = new SimpleDateFormat("yyyyMMMdd");



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
            AtomicLong numOfSensors = new AtomicLong((long) schemaDetail.get("number_of_sensors_per_location"));
            try {
                Date dateS = sdf.parse((String) schemaDetail.get("start_date"));
                Date dateE = sdf.parse((String) schemaDetail.get("end_date"));
                long startMs = dateS.getTime();
                long endMs = dateE.getTime();
                long diff = endMs - startMs;
                long totalDays = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                int approxMonths = (int) (totalDays/30);
                int remainingDays = (int) (totalDays%30);
                long msInADay = (24*60*60*1000);
                long msInaMonth = msInADay * 30;
                AtomicLong newEndMs = new AtomicLong();
                AtomicLong newStartMs = new AtomicLong(startMs);
                // Total iterations i.e. file for a sensor -> duration of 30 days for every sensor
                int totalIterationsForASensor = (int) (numOfSensors.longValue() * approxMonths) + ( remainingDays > 1 ? 1: 0);

                //parallel streaming for all sensors to write to the files
                IntStream.range(0, numOfSensors.intValue()).parallel().forEach( sensor -> {
                    // sequential write to files for a sensor for the time duration
                    IntStream.range(0, totalIterationsForASensor).forEach(i -> {
                        try {
//                            numOfSensors.set(numOfSensors.get()-i);
                            newEndMs.set(newStartMs.longValue() + msInaMonth);
                            long endDate = Math.min(newEndMs.longValue(), endMs);
                            csvFileForASensor(schemaDetail, sensor, newStartMs.longValue(), endDate);
                            newStartMs.set(endDate);

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                });

            } catch (java.text.ParseException e) {
                throw new RuntimeException(e);
            }



        }

        private void  csvFileForASensor(JSONObject schemaDetail, int sensorNum, long startDateMs, long endDateMs) throws IOException {
            //location of directory where data files for a given schema will be stored
//            File tmpDirectory = new File((String) schemaDetail.get("data_file_output_dir"));

            System.out.println("Creating CSV file for start: " + startDateMs + " End: " + endDateMs);
            String fileNameStartDt = sdfFileName.format(new Date(startDateMs));
            String fileNameEndDt = sdfFileName.format(new Date(endDateMs));

            Files.createDirectories(Paths.get(schemaDetail.get("data_file_output_dir") + "\\" + this.city + "\\" + sensorNum));
            // file name for each data file
            //TODO create file name

            File file = new File(schemaDetail.get("data_file_output_dir") + "\\" + this.city  + "\\" + sensorNum
                    + "\\" + schemaDetail.get("file_name_prefix")  +"-Sensor-" + sensorNum + "-Date-" + fileNameStartDt  + '-' + fileNameEndDt+  ".csv");
            file.createNewFile();

//            File file = File.createTempFile(schemaDetail.get("file_name_prefix") + "_", ".csv", tmpDirectory);

//            List<String[]> csvData = createCSVDataChunk(schemaDetail, sensorNum, startDateMs, endDateMs);

            long newStartDateMs = startDateMs;
            for(long i=0; i< endDateMs;) {
//                long newEndDateMs = startDateMs + 20000;
                long newEndDateMs = endDateMs;
                List<String[]> csvData = createCSVDataChunk(schemaDetail, sensorNum, newStartDateMs, newEndDateMs);
                newStartDateMs = newEndDateMs;

//                try (CSVWriter writer = new CSVWriter(new FileWriter(file) )) {
//                    writer.writeAll(csvData);
//                }

                try (CSVWriter writer = new CSVWriter(new FileWriter(file, true) )) {
                    long s = System.currentTimeMillis();
                    writer.writeAll(csvData);
                    long e = System.currentTimeMillis();
                    System.err.println("Time taken to write in milliseconds : " + (e-s));
                    writer.flush();
                }
                i  = newEndDateMs;
            }
//            try (CSVWriter writer = new CSVWriter(new FileWriter(file))) {
//                long s = System.currentTimeMillis();
//                writer.writeAll(csvData);
//                long e = System.currentTimeMillis();
//                System.err.println("Time taken to write in milliseconds : " + (e-s));
//            }


        }

        private List<String[]> createCSVDataChunk(JSONObject schemaDetail, int sensorNum, long msStart, long msEnd) {
            JSONParser parser = new JSONParser();
            JSONObject jsonSchemaObj;
            List<String> headers = new ArrayList<>();
            String schemaFilePath = (String) schemaDetail.get("schema");
            try (FileReader schemaReader = new FileReader(schemaFilePath)) {
                Object schemaObj = parser.parse(schemaReader);
                jsonSchemaObj = (JSONObject) schemaObj;
                Set keys = jsonSchemaObj.keySet();
                keys.forEach(key -> headers.add((String) key));
//                Date dateS = sdf.parse((String) schemaDetail.get("start_date"));
//                Date dateE = sdf.parse((String) schemaDetail.get("end_date"));
//                long msStart = dateS.getTime();
//                long msEnd = dateE.getTime();
                long totalMilliseconds = msEnd - msStart;
                long interval = (long) schemaDetail.get("interval");
                String intervalUnit = (String) schemaDetail.get("interval_unit");
                long totalRecordsPerSensor = calculateTotalRecordsPerSensor(interval, intervalUnit, totalMilliseconds);
                System.err.println("Total Records for a schema " + totalRecordsPerSensor);
                List<String[]> list = new ArrayList<>((int) totalRecordsPerSensor);
                String[] colNames = new String[headers.size()];
                colNames = headers.toArray(colNames);
                list.add(colNames);
                for(int i=0; i<totalRecordsPerSensor; i++) {
                    String [] record = generateRecord(jsonSchemaObj, headers, msStart,  interval, intervalUnit, i, sensorNum);
                    list.add(record);
                }
                System.out.println("Size : " + list.size());
                return list;


            } catch (IOException | ParseException e) {
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
                    return (totalMilliseconds)/interval;
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

        private String[] generateRecord(JSONObject jsonSchemaObj, List<String> headers, long msStart, long interval, String intervalUnit, int iteration, int sensorNum) {
            List<String> record = new ArrayList<>();
            HashMap<String, String> parentValues = new HashMap<>();
            String[] recordStrArray = new String[headers.size()];
            for( String header: headers) {
                String data;
                JSONObject details = (JSONObject) jsonSchemaObj.get(header);
                String type = (String) details.get("type");
                if( type.equals("timestamp")) {
                    String precision = (String) details.get("precision");
                    data = getTimestampVal(msStart, interval, intervalUnit, iteration, precision);
                } else if(header.equals("meter_id")) {
                    data = "MID-" + sensorNum;
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

            // calculate the increment from the start time in the given interval unit in the current iteration
            long incrementInIteration = (int) (interval * iteration);
            String incrementInIterationStr = incrementInIteration + "";
            int len = (incrementInIterationStr).length();
            switch(intervalUnit) {
                case "Nanosecond" :
                    randomTimeStamp = getRandomStrNs(interval, intervalUnit, precision, incrementInIteration, incrementInIterationStr, len);
                    return msStart + randomTimeStamp;
                case "Microsecond" :
                    randomTimeStamp = getRandomStrMs(interval, intervalUnit, precision, incrementInIteration, incrementInIterationStr, len);
                    return msStart + randomTimeStamp;
//                    if(incrementInIteration>=100 && incrementInIteration<=999) {
//                        randomTimeStamp = String.valueOf(incrementInIteration);
//                    } else if(incrementInIteration>=10 && incrementInIteration<=99) {
//                        randomTimeStamp = "0" + incrementInIteration;
//                    } else if(incrementInIteration>=1 && incrementInIteration<=9) {
//                        randomTimeStamp = "00" + incrementInIteration;
//                    } else {
//                        throw new RuntimeException("The interval value "+ interval + "is invalid for interval Unit " + intervalUnit);
//                    }
//                    return msStart + randomTimeStamp;
                case "Millisecond" :
                    randomTimeStamp = getRandomTimeStamp(precision, r, interval);
                    return (msStart + incrementInIteration) + randomTimeStamp;
                case "Second" :
                    randomTimeStamp = getRandomTimeStamp(precision, r, interval);
                    return msStart + (incrementInIteration * 1000) + randomTimeStamp;
                case "Minute" :
                    randomTimeStamp = getRandomTimeStamp(precision, r, interval);
                    return msStart + (incrementInIteration * 1000 * 60) + randomTimeStamp;
                case "Hour" :
                    randomTimeStamp = getRandomTimeStamp(precision, r, interval);
                    return msStart + (incrementInIteration * 1000 * 60 * 60) + randomTimeStamp;
                case "Day" :
                    randomTimeStamp = getRandomTimeStamp(precision, r, interval);
                    return msStart + (incrementInIteration * 1000 * 60 * 60 * 24) + randomTimeStamp;
                default:
                    throw new RuntimeException("The interval unit "+ intervalUnit + " is invalid or not supported");
            }
        }

        private String getRandomTimeStamp(String precision, Random r, long interval) {
            String randomTimeStamp;
            switch(precision) {
                case "Nanosecond" :
                    randomTimeStamp = String.valueOf((r.nextInt(999999-100000) + 100000));
                    break;
                case "Microsecond" :
                    randomTimeStamp =  String.valueOf((r.nextInt(999-100) + 100));
                    break;
                case "Millisecond" :
                    randomTimeStamp = "";
                    break;
                case "Second" :
                case "Minute" :
                case "Hour" :
                case "Day":
                    throw new RuntimeException("The interval value "+ interval + "is invalid for precision " + precision);
                default:
                    throw new RuntimeException("Unsupported interval "+ interval);
            }
            return randomTimeStamp;
        }

        private String getRandomStrMs(long interval, String intervalUnit, String precision, long incrementInIteration, String incrementInIterationStr, int len) {
            String randomTimeStamp;
            switch(precision) {
                case "Nanosecond" :
                    randomTimeStamp = StringUtils.leftPad(incrementInIterationStr, 3- len, '0');
                    if(incrementInIteration >999) {
                        throw new RuntimeException("The interval value "+ interval + "is invalid for interval Unit " + intervalUnit + " for the given time range");
                    }
                    break;
                case "Microsecond" :
                    randomTimeStamp = "";
                    break;
                case "Millisecond" :
                case "Second" :
                case "Minute" :
                case "Hour" :
                case "Day":
                    throw new RuntimeException("The interval value "+ interval + "is invalid for precision " + precision);
                default:
                    throw new RuntimeException("Unsupported interval "+ interval);
            }
            return randomTimeStamp;
        }

        private String getRandomStrNs(long interval, String intervalUnit, String precision, long incrementInIteration, String incrementInIterationStr, int len) {
            String randomTimeStamp;
            switch(precision) {
                case "Nanosecond" :
                    randomTimeStamp = StringUtils.leftPad(incrementInIterationStr, 6- len, '0');
                    if(incrementInIteration >999999) {
                        throw new RuntimeException("The interval value "+ interval + "is invalid for interval Unit " + intervalUnit);
                    }
                    break;
                case "Microsecond" :
                case "Millisecond" :
                case "Second" :
                case "Minute" :
                case "Hour" :
                case "Day":
                    throw new RuntimeException("The interval value "+ interval + "is invalid for precision " + precision);
                default:
                    throw new RuntimeException("Unsupported interval "+ interval);
            }
            return randomTimeStamp;
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
            double percent = details.get("percent")!=null ? (Double) details.get("percent") : 1;

            //if column has dependent columns i.e. child columns. So that such column value is stored for calculating the child col values.
            boolean hasDependents =  details.get("hasDependents")!=null && (boolean) details.get("hasDependents");

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
                    if(isPredefined) {
                        if(header.equals("city")) {
                            return this.city;
                        }
                        if(header.equals("location")){
                            return this.location;
                        }
                    }
                    return "Unknown";
                default:
                    //
                    return null;
            }
        }


    }

}
