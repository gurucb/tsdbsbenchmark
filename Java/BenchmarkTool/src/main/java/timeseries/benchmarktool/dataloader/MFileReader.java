package timeseries.benchmarktool.dataloader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Multithreading tool class for reading files by line
 *
 */

public class MFileReader {
    private int threadNum = 3; //thread number, default is 3
    private String filePath; //file path
    private int bufSize = 10; //buffer size, default is 1024
    private DataProcessHandler dataProcessHandler; //data processing interface
    private ExecutorService threadPool;

    public MFileReader(String filePath, int bufSize, int threadNum) {
        this.threadNum = threadNum;
        this.bufSize = bufSize;
        this.filePath = filePath;
        this.threadPool = Executors.newFixedThreadPool(threadNum);
    }
    

    /**
     * Start Multi threading to read files
     */
    public void startRead() {
        FileChannel infile = null;
        try {
            RandomAccessFile raf = new RandomAccessFile(filePath, "r");
            infile = raf.getChannel();
            long size = infile.size();
            System.out.println("FileSize"+size);
            long subSize = size / threadNum;
            for (int i = 0; i < threadNum; i++) {
                long startIndex = i * subSize;
                if (size % threadNum > 0 && i == threadNum - 1) {
                    subSize += size % threadNum;
                }
                RandomAccessFile accessFile = new RandomAccessFile(filePath, "r");
                FileChannel inch = accessFile.getChannel();
                System.out.println("startIndex: "+startIndex);
                System.out.println("subSize: "+subSize);
                
                threadPool.execute(new MultiThreadReader(inch, startIndex, subSize));
            }
            threadPool.shutdown();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (infile != null) {
                    infile.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Registration data processing interface
     * @param dataHandler
     */
    public void registerHanlder(DataProcessHandler dataHandler) {
        this.dataProcessHandler = dataHandler;
    }

    /**
     * The specific implementation class of multi-threaded reading files by line
     * @author zyh
     *
     */
    public class MultiThreadReader implements Runnable {
        private FileChannel channel;
        private long startIndex;
        private long rSize;

        public MultiThreadReader(FileChannel channel, long startIndex, long rSize) {
            this.channel = channel;
            this.startIndex = startIndex > 0 ? startIndex - 1 : startIndex;
            this.rSize = rSize;
        }

        public void run() {
            readByLine();
        }

        /**
         * Read files by line to achieve logic
         * @return
         */
        public void readByLine() {
            try {
                ByteBuffer rbuf = ByteBuffer.allocate(bufSize);
                channel.position(startIndex); //Set the starting position of reading the file
                long endIndex = startIndex + rSize; //The end position of reading file data
                byte[] temp = new byte[0]; //Used to buffer the remaining part of the last read
                int LF = "\n".getBytes()[0]; //Line feed
                boolean isEnd = false; //Used to determine whether the data has been read
                boolean isWholeLine = false; //Used to determine whether the first line read is a complete line
                long lineCount = 0; //line count
                long endLineIndex = startIndex; //The position of the current processed byte
                while (channel.read(rbuf) != -1 && !isEnd) {
                    int position = rbuf.position();
                    byte[] rbyte = new byte[position];
                    rbuf.flip();
                    rbuf.get(rbyte);
                    int startnum = 0; //The starting position index of each line, relative to the byte array currently read
                    //Determine whether there is a newline
                    //If the last line is not a complete line, continue to read until the complete line is read.
                    for (int i = 0; i < rbyte.length; i++) {
                        endLineIndex++;
                        if (rbyte[i] == LF) { //if there is a newline
                            if (channel.position() == startIndex) { //If the first byte of the data fragment is changed to a newline character, it means that the first line read is a complete line
                                isWholeLine = true;
                                startnum = i + 1;
                            } else {
                                byte[] line = new byte[temp.length + i - startnum + 1];
                                System.arraycopy(temp, 0, line, 0, temp.length);
                                System.arraycopy(rbyte, startnum, line, temp.length, i - startnum + 1);
                                startnum = i + 1;
                                lineCount++;
                                temp = new byte[0];
                                //Data processing
                                if (startIndex != 0) { //if not the first data segment
                                    if (lineCount == 1) {
                                        if (isWholeLine) { //Process if and only if the first line is a complete line
                                            dataProcessHandler.process(line);
                                        }
                                    } else {
                                        dataProcessHandler.process(line);
                                    }
                                } else {
                                    dataProcessHandler.process(line);
                                }
                                //Judgment to end reading
                                if (endLineIndex >= endIndex) {
                                    isEnd = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!isEnd && startnum < rbyte.length) { //indicates that rbyte has an incomplete line at the end
                        byte[] temp2 = new byte[temp.length + rbyte.length - startnum];
                        System.arraycopy(temp, 0, temp2, 0, temp.length);
                        System.arraycopy(rbyte, startnum, temp2, temp.length, rbyte.length - startnum);
                        temp = temp2;
                    }
                    rbuf.clear();
                }
                //Compatible with no line break in the last line
                if (temp.length > 0) {
                    if (dataProcessHandler != null) {
                        dataProcessHandler.process(temp);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int getThreadNum() {
        return threadNum;
    }

    public String getFilePath() {
        return filePath;
    }

    public ExecutorService getThreadPool() {
        return threadPool;
    }
    public int getBufSize() {
        return bufSize;
    }
}