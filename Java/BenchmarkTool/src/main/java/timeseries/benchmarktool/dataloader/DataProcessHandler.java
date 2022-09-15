package timeseries.benchmarktool.dataloader;

/**
 * data processing interface
 * @author zyh
 *
 */
public interface DataProcessHandler {
    void process(byte[] data);
}