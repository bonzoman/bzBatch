package com.bzbatch.common.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileMerger {

    public static void mergeFilesInParallel(List<Path> inputFiles, Path outputFile) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
            inputFiles.parallelStream()
                    .flatMap(file -> {
                        try {
                            return Files.lines(file);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .forEachOrdered(line -> {
                        try {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    public static void mergeFilesDelete(List<Path> inputFiles) throws IOException {
        for (Path file : inputFiles) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                System.err.println("Failed to delete file: " + file + " -> " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        List<Path> files = List.of(
                Paths.get("D://batchlog/output_01.txt"),
                Paths.get("D://batchlog/output_02.txt"),
                Paths.get("D://batchlog/output_03.txt")
                // ... 나머지 7개
        );
        Path output = Paths.get("D://batchlog/merged.csv");
        mergeFilesInParallel(files, output);
    }
}
