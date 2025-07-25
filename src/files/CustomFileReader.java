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
    public static String INPUT_FOLDER = "src/DataSet/input_folder";
    public static String OUTPUT_FOLDER = "src/DataSet/output_folder";

    private ExecutorService ioPool;          
    private ExecutorService processingPool;  
    private AtomicInteger filesToProcess;    
    private BlockingQueue<Boolean> overallCompletionSignal; 

    public CustomFileReader() {
        this.ioPool = Executors.newFixedThreadPool(Math.min(4, Runtime.getRuntime().availableProcessors()));
        this.processingPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        this.filesToProcess = new AtomicInteger(0);
        this.overallCompletionSignal = new ArrayBlockingQueue<>(1);
    }

    public void processFolder() throws IOException, InterruptedException {
        Path inputPath = Paths.get(INPUT_FOLDER);
        Path outputPath = Paths.get(OUTPUT_FOLDER);

        System.out.println("[CustomFileReader] Starting processFolder for INPUT_FOLDER: " + inputPath.toAbsolutePath());
     
        if (Files.exists(outputPath)) {
            System.out.println("[CustomFileReader] Output path exists. Cleaning: " + outputPath.toAbsolutePath());
            Files.walk(outputPath)
                 .sorted((p1, p2) -> -p1.toString().compareTo(p2.toString())) 
                 .forEach(p -> {
                     try {
                         Files.delete(p);
                     } catch (IOException e) {
                         System.err.println("Could not delete " + p + ": " + e.getMessage());
                     }
                 });
        }
        Files.createDirectories(outputPath);
        System.out.println("[CustomFileReader] Output directory ensured: " + outputPath.toAbsolutePath());

        System.out.println("[CustomFileReader] Starting to walk file tree for: " + INPUT_FOLDER);

        Files.walkFileTree(inputPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                System.out.println("[walkFileTree] Visited file: " + file.getFileName());
                System.out.println("[walkFileTree] Is it a regular file? " + attrs.isRegularFile());
                if (attrs.isRegularFile()) {
                    filesToProcess.incrementAndGet(); 
                    System.out.println("[walkFileTree] filesToProcess incremented to: " + filesToProcess.get());
                    processSingleFile(file); 
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                System.out.println("[walkFileTree] Entering directory: " + dir.getFileName());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                System.out.println("[walkFileTree] Exiting directory: " + dir.getFileName());
                if (dir.equals(inputPath)) {
                    System.out.println("[CustomFileReader] All files in folder discovered. Waiting for processing to complete...");
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                System.err.println("[walkFileTree] Failed to visit file " + file + ": " + exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });

        System.out.println("[CustomFileReader] walkFileTree finished. Initial filesToProcess count: " + filesToProcess.get());
        if (filesToProcess.get() == 0) {
            System.out.println("[CustomFileReader] No regular files found in " + INPUT_FOLDER + ". Signalling immediate completion.");
            overallCompletionSignal.put(true);
        }

        overallCompletionSignal.take();
        System.out.println("Finished processing all files in folder.");

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

        System.out.println("[processSingleFile] Processing: " + inputFile.getFileName() + " to " + outputFile.toAbsolutePath());

        Files.createDirectories(outputFile.getParent());

        AsynchronousFileChannel inputChannel = null;
        AsynchronousFileChannel outputChannel = null;

        try {
            inputChannel = AsynchronousFileChannel.open(inputFile, StandardOpenOption.READ);
            outputChannel = AsynchronousFileChannel.open(outputFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            long inputFileSize = inputChannel.size();
            System.out.println("Processing file: " + inputFile.getFileName() + ", size: " + inputFileSize + " bytes");

            if (inputFileSize == 0) {
                System.out.println("[processSingleFile] Warning: File " + inputFile.getFileName() + " is 0 bytes. It will be marked as complete immediately.");
                FileProcessingState emptyFileState = new FileProcessingState(
                        inputChannel, outputChannel, inputFileSize, inputFile.getFileName().toString());
                emptyFileState.allReadsInitiated = true;
                checkFileCompletion(emptyFileState);
                return;
            }

            FileProcessingState fileState = new FileProcessingState(
                    inputChannel, outputChannel, inputFileSize, inputFile.getFileName().toString());

            readNextBlock(fileState);

        } catch (IOException e) {
            System.err.println("Error opening channels for file " + inputFile + ": " + e.getMessage());
            FileProcessingState dummyState = new FileProcessingState(inputChannel, outputChannel, 0, inputFile.getFileName().toString());
            fileFailed(null, dummyState, e);
        }
    }

    private void readNextBlock(FileProcessingState fileState) {
        final long currentBlockStartPosition = fileState.currentReadPosition.getAndAdd(BLOCK_SIZE);

        if (currentBlockStartPosition >= fileState.inputFileSize) {
            // All read operations for this file have been initiated (or no more are needed).
            // This is the point where we know we've covered the entire file for reads.
            System.out.println("[readNextBlock] All reads INITIATED for file: " + fileState.fileName + ". Current pos: " + currentBlockStartPosition + ", File size: " + fileState.inputFileSize); // Debug
            fileState.allReadsInitiated = true; // <-- Moved this line here!
            checkFileCompletion(fileState); // Check if this file is fully done (all writes might be done too)
            return;
        }
        System.out.println("[readNextBlock] Initiating read for " + fileState.fileName + " at position " + currentBlockStartPosition);

        ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
        ReadAttachment attachment = new ReadAttachment(buffer, currentBlockStartPosition, fileState);

        fileState.inputChannel.read(buffer, currentBlockStartPosition, attachment, new CompletionHandler<Integer, ReadAttachment>() {
            @Override
            public void completed(Integer bytesRead, ReadAttachment attach) {
                System.out.println("[read.completed] Read " + bytesRead + " bytes for " + attach.getFileState().fileName + " at " + attach.getStartPosition());
                if (bytesRead < 0) { // Actual End of File detected by the read operation
                    System.out.println("[read.completed] EOF detected for " + attach.getFileState().fileName);
                    // No need to set allReadsInitiated here again, it should already be true.
                    checkFileCompletion(fileState); // Re-check completion
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
        String originalContent = Charset.defaultCharset().decode(inputBuffer).toString();
        String modifiedContent = originalContent.toUpperCase(); 
        ByteBuffer outputBuffer = Charset.defaultCharset().encode(modifiedContent);

        System.out.println("[processBuffer] Processed chunk for " + fileName + " from position " + startPosition + ". Original size: " + inputBuffer.limit() + ", Processed size: " + outputBuffer.limit());
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

            System.out.println("[attemptWrite] Initiating write for " + fileState.fileName + " at position " + writePos + " (size: " + writtenBytesInThisBlock + ")");

            fileState.outputChannel.write(bufferToWrite, writePos, bufferToWrite, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    System.out.println("[write.completed] Written " + result + " bytes for " + fileState.fileName + " at position " + writePos);
                    fileState.totalBytesWritten.addAndGet(result); 
                    checkFileCompletion(fileState); 
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    System.err.println("Write failed for file " + fileState.fileName + " at position " + writePos + ": " + exc.getMessage());
                    fileFailed(attachment, fileState, exc);
                }
            });
        }
    }

   
    private void checkFileCompletion(FileProcessingState fileState) {
        boolean allReadsDone = fileState.allReadsInitiated;
        boolean processedBuffersEmpty = fileState.processedBuffers.isEmpty();
        long currentNextWritePosition = fileState.nextWritePosition.get();
        long fileTotalSize = fileState.inputFileSize;

        System.out.println("[checkFileCompletion] Checking " + fileState.fileName + ": " +
                           "allReadsDone=" + allReadsDone +
                           ", processedBuffersEmpty=" + processedBuffersEmpty +
                           ", nextWritePos=" + currentNextWritePosition +
                           ", fileTotalSize=" + fileTotalSize +
                           ", isNextWritePosFinal=" + (currentNextWritePosition >= fileTotalSize));

        if (allReadsDone && processedBuffersEmpty && currentNextWritePosition >= fileTotalSize) {
            System.out.println("[checkFileCompletion] ALL CONDITIONS MET for " + fileState.fileName);
            try {
                if (fileState.inputChannel.isOpen()) fileState.inputChannel.close();
                if (fileState.outputChannel.isOpen()) fileState.outputChannel.close();
                System.out.println("Finished processing and writing file: " + fileState.fileName);
                filesToProcess.decrementAndGet(); 
                System.out.println("[checkFileCompletion] filesToProcess decremented to: " + filesToProcess.get()); 
                checkAllFilesCompletion(); 
            } catch (IOException e) {
                System.err.println("Error closing channels for " + fileState.fileName + ": " + e.getMessage());
                filesToProcess.decrementAndGet();
                checkAllFilesCompletion();
            }
        } else {
            System.out.println("[checkFileCompletion] Conditions NOT YET MET for " + fileState.fileName);
        }
    }


    private void fileFailed(ByteBuffer buffer, FileProcessingState fileState, Throwable cause) {
        System.err.println("File processing failed for " + fileState.fileName + ": " + cause.getMessage());
        try {
            if (fileState.inputChannel != null && fileState.inputChannel.isOpen()) fileState.inputChannel.close();
            if (fileState.outputChannel != null && fileState.outputChannel.isOpen()) fileState.outputChannel.close();
           
            Path inputFileParent = Paths.get(INPUT_FOLDER);
            Path outputFileToDelete = Paths.get(OUTPUT_FOLDER).resolve(inputFileParent.relativize(Paths.get(fileState.fileName)));
            Files.deleteIfExists(outputFileToDelete);
            System.err.println("Attempted to delete partial output: " + outputFileToDelete);
        } catch (IOException e) {
            System.err.println("Error cleaning up after file failure for " + fileState.fileName + ": " + e.getMessage());
        } finally {
            filesToProcess.decrementAndGet(); 
            System.out.println("[fileFailed] filesToProcess decremented to: " + filesToProcess.get());
            checkAllFilesCompletion();
        }
    }


    private void checkAllFilesCompletion() {
        if (filesToProcess.get() == 0) {
            System.out.println("[checkAllFilesCompletion] All files processed. Signalling overall completion.");
            try {
                overallCompletionSignal.put(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    private static class FileProcessingState {
        final AsynchronousFileChannel inputChannel;
        final AsynchronousFileChannel outputChannel;
        final long inputFileSize;
        final String fileName; 

        final AtomicLong currentReadPosition = new AtomicLong(0);
        final ConcurrentSkipListMap<Long, ByteBuffer> processedBuffers = new ConcurrentSkipListMap<>();
        final AtomicLong nextWritePosition = new AtomicLong(0);
        final AtomicLong totalBytesWritten = new AtomicLong(0); 
        volatile boolean allReadsInitiated = false; 

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

    public static void createDummyFolderWithFiles(String folderPath, int numberOfFiles, long fileSizePerFile) {
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