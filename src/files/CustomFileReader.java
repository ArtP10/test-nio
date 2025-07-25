package files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CustomFileReader {

    private static final int BLOCK_SIZE = 64 * 1024; // 64 KB
    static String INPUT_FOLDER = "src/DataSet/input_folder";
    static String OUTPUT_FOLDER = "src/DataSet/output_folder";

    private ExecutorService ioPool;          // For I/O CompletionHandlers
    private ExecutorService processingPool;  // For CPU-bound data manipulation
    private AtomicInteger filesToProcess;    // Counter for remaining files
    private BlockingQueue<Boolean> overallCompletionSignal; // To signal when ALL files are done

    public CustomFileReader() {
        // A dedicated pool for I/O completion handlers (optional, can use default)
        // Using a small fixed pool for I/O can prevent I/O threads from being saturated by processing logic
        this.ioPool = Executors.newFixedThreadPool(Math.min(4, Runtime.getRuntime().availableProcessors()));
        // A larger pool for CPU-bound processing
        this.processingPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        this.filesToProcess = new AtomicInteger(0);
        this.overallCompletionSignal = new ArrayBlockingQueue<>(1);
    }

    public void processFolder() throws IOException, InterruptedException {
        Path inputPath = Paths.get(INPUT_FOLDER);
        Path outputPath = Paths.get(OUTPUT_FOLDER);

        // Ensure output directory exists and is clean
        if (Files.exists(outputPath)) {
            Files.walk(outputPath)
                 .sorted((p1, p2) -> -p1.toString().compareTo(p2.toString())) // Delete from deepest first
                 .forEach(p -> {
                     try {
                         Files.delete(p);
                     } catch (IOException e) {
                         System.err.println("Could not delete " + p + ": " + e.getMessage());
                     }
                 });
        }
        Files.createDirectories(outputPath);

        System.out.println("Starting to process folder: " + INPUT_FOLDER);

        // Discover files and start processing them
        Files.walkFileTree(inputPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isRegularFile()) {
                    filesToProcess.incrementAndGet(); // Increment counter for each file found
                    processSingleFile(file); // Start asynchronous processing for this file
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (dir.equals(inputPath)) {
                    // All files have been discovered and processing initiated.
                    // Now, wait for all completion signals.
                    // No need to signal completion here directly, as it's handled by checkAllFilesCompletion.
                    System.out.println("All files in folder discovered. Waiting for processing to complete...");
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                System.err.println("Failed to visit file " + file + ": " + exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });

        // Wait for all files to be processed
        overallCompletionSignal.take();
        System.out.println("Finished processing all files in folder.");

        // Shut down pools
        ioPool.shutdown();
        processingPool.shutdown();
        try {
            if (!ioPool.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("I/O pool did not terminate in time.");
            }
            if (!processingPool.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("Processing pool did not terminate in time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processSingleFile(Path inputFile) throws IOException {
        Path relativePath = Paths.get(INPUT_FOLDER).relativize(inputFile);
        Path outputFile = Paths.get(OUTPUT_FOLDER).resolve(relativePath);

        // Ensure output directory structure is mirrored
        Files.createDirectories(outputFile.getParent());

        AsynchronousFileChannel inputChannel = null;
        AsynchronousFileChannel outputChannel = null;

        try {
            inputChannel = AsynchronousFileChannel.open(inputFile, StandardOpenOption.READ);
            outputChannel = AsynchronousFileChannel.open(outputFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            long inputFileSize = inputChannel.size();
            System.out.println("Processing file: " + inputFile.getFileName() + ", size: " + inputFileSize + " bytes");

            // Create a specific state object for this file's processing
            FileProcessingState fileState = new FileProcessingState(
                    inputChannel, outputChannel, inputFileSize, inputFile.getFileName().toString());

            // Start the first read for this file
            readNextBlock(fileState);

        } catch (IOException e) {
            System.err.println("Error opening channels for file " + inputFile + ": " + e.getMessage());
            
        }
    }

    private void readNextBlock(FileProcessingState fileState) {
        final long currentBlockStartPosition = fileState.currentReadPosition.getAndAdd(BLOCK_SIZE);

        if (currentBlockStartPosition >= fileState.inputFileSize) {
            // All read operations for this file have been initiated
            // The file completion will be signaled when all writes for this file are done
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
        ReadAttachment attachment = new ReadAttachment(buffer, currentBlockStartPosition, fileState);

        fileState.inputChannel.read(buffer, currentBlockStartPosition, attachment, new CompletionHandler<Integer, ReadAttachment>() {
            @Override
            public void completed(Integer bytesRead, ReadAttachment attach) {
                if (bytesRead < 0) { // End of stream for this file
                    // No more reads to initiate for this file.
                    fileState.allReadsInitiated = true; // Mark that all reads for this file are launched
                    checkFileCompletion(fileState); // Check if this file is fully done
                    return;
                }

                if (bytesRead < attach.getBuffer().capacity()) {
                    attach.getBuffer().limit(bytesRead);
                }
                attach.getBuffer().flip();

                processingPool.submit(() -> {
                    try {
                        ByteBuffer processedBuffer = processBuffer(attach.getBuffer(), attach.getStartPosition(), attach.getFileState().fileName);
                        attach.getFileState().processedBuffers.put(attach.getStartPosition(), processedBuffer);
                        attemptWrite(attach.getFileState());
                    } catch (Exception e) {
                        fileFailed(attach.getBuffer(), attach.getFileState(), e);
                    }
                });

                // Initiate the next read operation for THIS file
                readNextBlock(fileState);
            }

            @Override
            public void failed(Throwable exc, ReadAttachment attach) {
                System.err.println("Read failed for file " + attach.getFileState().fileName + " at position " + attach.getStartPosition() + ": " + exc.getMessage());
                fileFailed(attach.getBuffer(), attach.getFileState(), exc);
            }
        });
    }

    private ByteBuffer processBuffer(ByteBuffer inputBuffer, long startPosition, String fileName) {
        // --- YOUR MANIPULATION LOGIC GOES HERE ---
        // Example: Convert content to uppercase
        String originalContent = Charset.defaultCharset().decode(inputBuffer).toString();
        String modifiedContent = originalContent.toUpperCase(); // Simple manipulation
        ByteBuffer outputBuffer = Charset.defaultCharset().encode(modifiedContent);

        // System.out.println("Processed chunk for " + fileName + " from position " + startPosition);
        inputBuffer.clear();
        return outputBuffer;
    }

    private synchronized void attemptWrite(FileProcessingState fileState) {
        while (true) {
            ByteBuffer bufferToWrite = fileState.processedBuffers.get(fileState.nextWritePosition.get());
            if (bufferToWrite == null) {
                break;
            }

            final long writePos = fileState.nextWritePosition.get();
            final long writtenBytesInThisBlock = bufferToWrite.limit();

            fileState.processedBuffers.remove(writePos);
            fileState.nextWritePosition.addAndGet(writtenBytesInThisBlock);

            fileState.outputChannel.write(bufferToWrite, writePos, bufferToWrite, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    // System.out.println("Written " + result + " bytes for " + fileState.fileName + " at position " + writePos);
                    fileState.totalBytesWritten.addAndGet(result); // Track total bytes written for this file
                    checkFileCompletion(fileState); // Check if this file is done writing
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    System.err.println("Write failed for file " + fileState.fileName + " at position " + writePos + ": " + exc.getMessage());
                    fileFailed(attachment, fileState, exc);
                }
            });
        }
    }

    // Checks if a single file has completed its processing and writing
    private void checkFileCompletion(FileProcessingState fileState) {
        // This logic can be tricky if output file size differs from input file size.
        // Assuming output size == input size for now.
        // If output size can differ, you need to track total expected output bytes or just rely on outstanding writes.
        if (fileState.allReadsInitiated && fileState.processedBuffers.isEmpty() && fileState.nextWritePosition.get() >= fileState.inputFileSize) {
             // If the file was empty, inputFileSize is 0, nextWritePosition is 0. This condition handles it.
            try {
                if (fileState.inputChannel.isOpen()) fileState.inputChannel.close();
                if (fileState.outputChannel.isOpen()) fileState.outputChannel.close();
                System.out.println("Finished processing and writing file: " + fileState.fileName);
                filesToProcess.decrementAndGet(); // Decrement the global counter
                checkAllFilesCompletion(); // Check if all files are done
            } catch (IOException e) {
                System.err.println("Error closing channels for " + fileState.fileName + ": " + e.getMessage());
                // This is an unusual error, but treat it as a failure for this file
                filesToProcess.decrementAndGet();
                checkAllFilesCompletion();
            }
        }
    }

    // Handles a failure for a specific file
    private void fileFailed(ByteBuffer buffer, FileProcessingState fileState, Throwable cause) {
        System.err.println("File processing failed for " + fileState.fileName + ": " + cause.getMessage());
        try {
            if (fileState.inputChannel != null && fileState.inputChannel.isOpen()) fileState.inputChannel.close();
            if (fileState.outputChannel != null && fileState.outputChannel.isOpen()) fileState.outputChannel.close();
            // Delete partially written output file if desired
            Files.deleteIfExists(Paths.get(OUTPUT_FOLDER).resolve(Paths.get(INPUT_FOLDER).relativize(Paths.get(fileState.fileName))));
        } catch (IOException e) {
            System.err.println("Error cleaning up after file failure for " + fileState.fileName + ": " + e.getMessage());
        } finally {
            filesToProcess.decrementAndGet(); // Decrement regardless of success/failure
            checkAllFilesCompletion();
        }
    }

    // Checks if all files in the folder have completed processing
    private void checkAllFilesCompletion() {
        if (filesToProcess.get() == 0) {
            try {
                overallCompletionSignal.put(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // --- Helper classes for state management ---

    // State for a single file's processing
    private static class FileProcessingState {
        final AsynchronousFileChannel inputChannel;
        final AsynchronousFileChannel outputChannel;
        final long inputFileSize;
        final String fileName; // For logging

        final AtomicLong currentReadPosition = new AtomicLong(0);
        final ConcurrentSkipListMap<Long, ByteBuffer> processedBuffers = new ConcurrentSkipListMap<>();
        final AtomicLong nextWritePosition = new AtomicLong(0);
        final AtomicLong totalBytesWritten = new AtomicLong(0); // Useful if output size changes
        volatile boolean allReadsInitiated = false; // Flag to know when all reads for THIS file are launched

        FileProcessingState(AsynchronousFileChannel inputChannel, AsynchronousFileChannel outputChannel, long inputFileSize, String fileName) {
            this.inputChannel = inputChannel;
            this.outputChannel = outputChannel;
            this.inputFileSize = inputFileSize;
            this.fileName = fileName;
        }
    }

    private static class ReadAttachment {
        private final ByteBuffer buffer;
        private final long startPosition;
        private final FileProcessingState fileState;

        public ReadAttachment(ByteBuffer buffer, long startPosition, FileProcessingState fileState) {
            this.buffer = buffer;
            this.startPosition = startPosition;
            this.fileState = fileState;
        }

        public ByteBuffer getBuffer() { return buffer; }
        public long getStartPosition() { return startPosition; }
        public FileProcessingState getFileState() { return fileState; }
    }

    

    // Helper method to create a dummy folder with multiple files
    private static void createDummyFolderWithFiles(String folderPath, int numberOfFiles, long fileSizePerFile) {
        Path folder = Paths.get(folderPath);
        try {
            if (Files.exists(folder)) {
                Files.walk(folder)
                     .sorted((p1, p2) -> -p1.toString().compareTo(p2.toString()))
                     .forEach(p -> {
                         try {
                             Files.delete(p);
                         } catch (IOException e) {
                             System.err.println("Could not delete " + p + ": " + e.getMessage());
                         }
                     });
            }
            Files.createDirectories(folder);

            for (int i = 0; i < numberOfFiles; i++) {
                String fileName = "file_" + i + ".txt";
                Path filePath = folder.resolve(fileName);
                try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    String content = "This is content for file " + i + ". This is a line of repeatable content. ";
                    ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(Charset.defaultCharset()));
                    long written = 0;
                    while (written < fileSizePerFile) {
                        buffer.rewind();
                        int bytesToWrite = (int) Math.min(buffer.remaining(), fileSizePerFile - written);
                        if (bytesToWrite > 0) {
                            ByteBuffer currentBuffer = buffer.duplicate();
                            currentBuffer.limit(bytesToWrite);
                            Future<Integer> writeOp = channel.write(currentBuffer, written);
                            written += writeOp.get();
                        } else {
                            break;
                        }
                    }
                    System.out.println("Created dummy file: " + fileName + " with size " + written + " bytes.");
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}