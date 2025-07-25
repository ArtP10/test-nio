package files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class NewFileReader {

    // A more reasonable block size (e.g., 64KB)
    // Adjust based on your system's optimal I/O block size and available memory
    private static final int BLOCK_SIZE = 64 * 1024; // 64 KB

    static String FILENAME = "src/DataSet/large_file.txt";

    private AsynchronousFileChannel asyncChannel;
    private AtomicLong currentPosition = new AtomicLong(0); // Use AtomicLong for thread-safe position tracking
    private long fileSize; // To store the total file size
    private ExecutorService processingPool; // For processing the data off the I/O thread
    private BlockingQueue<Boolean> completionSignal; // To signal when reading is done

    public NewFileReader() {
        // Create a thread pool for processing the data.
        // Adjust the number of threads based on your CPU cores and processing needs.
        this.processingPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.completionSignal = new ArrayBlockingQueue<>(1);
    }

    public void readLargeFile() throws IOException, InterruptedException {
        Path path = Paths.get(FILENAME);
        // Open the file for reading. StandardOpenOption.READ is implicit, but good to be explicit.
        // Passing a null ExecutorService means the default I/O thread pool will be used for I/O operations.
        asyncChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        fileSize = asyncChannel.size();

        System.out.println("Starting to read file: " + FILENAME + ", size: " + fileSize + " bytes");

        // Start the first read operation
        readNextBlock();

        // Wait for the entire file to be read
        completionSignal.take();
        System.out.println("Finished reading file.");

        // Shut down the processing pool
        processingPool.shutdown();
        try {
            if (!processingPool.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Processing pool did not terminate in time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Close the channel
        asyncChannel.close();
    }

    private void readNextBlock() {
        final long currentReadPosition = currentPosition.getAndAdd(BLOCK_SIZE);

        // If we've read past the end of the file, we're done (or close to it)
        if (currentReadPosition >= fileSize) {
            // Signal completion. This might be called multiple times if there are
            // many concurrent reads finishing, but that's fine for ArrayBlockingQueue(1).
            try {
                completionSignal.put(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        // Allocate a new buffer for each read operation
        ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);

        // Define a custom attachment to pass context to the CompletionHandler
        // We could pass more info here if needed, like a block ID
        ReadAttachment attachment = new ReadAttachment(buffer, currentReadPosition);

        // Initiate the asynchronous read operation
        asyncChannel.read(buffer, currentReadPosition, attachment, new CompletionHandler<Integer, ReadAttachment>() {
            @Override
            public void completed(Integer bytesRead, ReadAttachment attach) {
                if (bytesRead < 0) { // End of stream
                    try {
                        completionSignal.put(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return;
                }

                // If fewer bytes were read than BLOCK_SIZE, it means we're at the end of the file
                if (bytesRead < attach.getBuffer().capacity()) {
                    attach.getBuffer().limit(bytesRead); // Adjust buffer limit to actual bytes read
                }

                attach.getBuffer().flip(); // Prepare buffer for reading

                // Submit the buffer for processing in the processing pool
                processingPool.submit(() -> processBuffer(attach.getBuffer(), attach.getStartPosition()));

                // Initiate the next read operation
                readNextBlock();
            }

            @Override
            public void failed(Throwable exc, ReadAttachment attach) {
                System.err.println("Read failed at position " + attach.getStartPosition() + ": " + exc.getMessage());
                try {
                    completionSignal.put(false); // Signal failure
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * This method processes the ByteBuffer.
     * This is where you would put your actual file processing logic.
     * It runs in a separate thread from the I/O thread.
     *
     * @param buffer The ByteBuffer containing the read data.
     * @param startPosition The starting position of this block in the file.
     */
    private void processBuffer(ByteBuffer buffer, long startPosition) {
        // For demonstration, we'll just print a message.
        // In a real application, you would parse, analyze, or transform the data.
        String chunk = Charset.defaultCharset().decode(buffer).toString();
        // System.out.println("Processed chunk from position " + startPosition + ", size: " + chunk.length());
        // Do *not* print the entire chunk for a large file here, unless it's for specific debugging.
        // For example, if you're looking for a specific pattern:
        // if (chunk.contains("some_keyword")) {
        //     System.out.println("Found keyword in chunk at position: " + startPosition);
        // }

        // It's crucial to clear the buffer if you intend to reuse it,
        // but here we are allocating a new one for each read, so it's less critical.
        // However, if you were pooling buffers, buffer.clear() would be essential.
        buffer.clear();
    }

    // Helper class to pass multiple arguments to the CompletionHandler
    private static class ReadAttachment {
        private final ByteBuffer buffer;
        private final long startPosition;

        public ReadAttachment(ByteBuffer buffer, long startPosition) {
            this.buffer = buffer;
            this.startPosition = startPosition;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public long getStartPosition() {
            return startPosition;
        }
    }

    public static void main(String[] args) {
        // Create a dummy large file for testing
        createDummyLargeFile(FILENAME, 1024 * 1024 * 10); // 10 MB for faster testing
        // For a 1GB file, use: createDummyLargeFile(FILENAME, 1024L * 1024 * 1024);

        NewFileReader reader = new NewFileReader();
        long startTime = System.nanoTime();
        try {
            reader.readLargeFile();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1_000_000; // milliseconds
        System.out.println("Total reading and processing time: " + duration + " ms");
    }

    // Helper method to create a large dummy file for testing
    private static void createDummyLargeFile(String filename, long sizeBytes) {
        Path path = Paths.get(filename);
        try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            String content = "This is a line of repeatable content for a large file. ";
            ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(Charset.defaultCharset()));
            long written = 0;
            Future<Integer> writeOp = null;

            while (written < sizeBytes) {
                buffer.rewind();
                int bytesToWrite = (int) Math.min(buffer.remaining(), sizeBytes - written);
                if (bytesToWrite > 0) {
                    ByteBuffer currentBuffer = buffer.duplicate();
                    currentBuffer.limit(bytesToWrite);
                    writeOp = channel.write(currentBuffer, written);
                    written += writeOp.get(); // Wait for the write to complete
                } else {
                    break;
                }
            }
            System.out.println("Dummy file created: " + filename + " with size " + written + " bytes.");
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}