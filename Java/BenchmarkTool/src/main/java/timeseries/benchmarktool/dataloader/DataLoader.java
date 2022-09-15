/**
 * 
 */
package timeseries.benchmarktool.dataloader;

import org.json.simple.JSONObject;

/**
 * @author bhjaiswal
 *
 */
public interface DataLoader {
	public void init(JSONObject config);
	public void read();
	public void write();
	public void cleanup();
}
