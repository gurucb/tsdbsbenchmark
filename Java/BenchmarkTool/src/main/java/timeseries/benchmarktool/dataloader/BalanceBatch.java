package timeseries.benchmarktool.dataloader;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

class BalanceBatch {
    private final List<RunnableBatch> threads = new ArrayList<>();

    private Queue<String> queue = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = 1;
    public BalanceBatch(int nbThread) {
        IntStream.range(0, nbThread).mapToObj(i -> new RunnableBatch(BATCH_SIZE, queue)).forEach(threads::add);
    }

    public void send(String value) {
        queue.add(value);
    }

    public void startAll() {
        for (RunnableBatch t : threads) {
            new Thread(t).start();
        }
    }

    public void stopAll() {
        for (RunnableBatch t : threads) {
            t.stop();
        }
    }
}
class RunnableBatch implements Runnable {

    private boolean started = true;
    private Queue<String> queue;
    private int batchLimit;

    public RunnableBatch(int batchLimit, Queue<String> queue) {
        this.batchLimit = batchLimit;
        this.queue = queue;
    }

    @Override
    public void run() {
        try (BatchInsert batch = new BatchInsert(batchLimit)) {
            while (!queue.isEmpty() || started) {
                String s = queue.poll();
                if (s == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {

                    }
                } else {
                    batch.insert(s);
                }
            }
        }
    }

    public void stop() {
        started = false;
    }
}