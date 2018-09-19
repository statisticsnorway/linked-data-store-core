package no.ssb.lds.core.saga;

import no.ssb.saga.api.Saga;
import no.ssb.saga.execution.sagalog.SagaLog;
import no.ssb.saga.execution.sagalog.SagaLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileSagaLog implements SagaLog {

    private static final Logger LOG = LoggerFactory.getLogger(FileSagaLog.class);

    private final Object lock = new Object();

    /*
     * Access to any of the following members must be protected by a
     * synchronized(lock) statement.
     */
    private long maxEntriesBeforeRotation;
    private boolean rotationInProgress = false;
    private long entries = 0;
    private File sagaLogFile;
    private final Set<String> openSagas = new TreeSet<>(); // saga-executions started but not yet ended
    private BufferedWriter logWriter;

    FileSagaLog(File sagaLogFile, long maxEntriesBeforeRotation) {
        this.maxEntriesBeforeRotation = (maxEntriesBeforeRotation > 0) ? maxEntriesBeforeRotation : 10000;
        synchronized (lock) {
            this.sagaLogFile = sagaLogFile;
            if (!sagaLogFile.exists()) {
                try {
                    Path path = currentPath().resolve(sagaLogFile.getParent());
                    Files.createDirectories(path);
                    sagaLogFile.createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            rotateAndRecoverLog();
        }
    }

    long getMaxEntriesBeforeRotation() {
        synchronized (lock) {
            return maxEntriesBeforeRotation;
        }
    }

    private static Path currentPath() {
        return Paths.get(".").toAbsolutePath().normalize();
    }

    private void rotateAndRecoverLog() {
        Path rotatedTo = rotate();
        if (rotatedTo != null) {
            openSagas.clear();
            Map<String, List<SagaLogEntry>> incompleteSagas = readAllIncompleteSagas(rotatedTo.toFile());
            incompleteSagas.forEach((executionId, entries) -> {
                entries.forEach(entry -> write(entry)); // will also update openSagas
            });
        }
    }

    public void close() {
        synchronized (lock) {
            if (logWriter != null) {
                try {
                    logWriter.close();
                    logWriter = null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    Set<String> snapshotOpenSagaExecutionIds() {
        synchronized (lock) {
            return new TreeSet<>(openSagas);
        }
    }

    /**
     * @return the archive file that the current log was rotated to or the newest archived file
     * if current-log does not exist, or null if no current nor archived saga-log files were found.
     */
    private Path rotate() {
        close();
        File parentFolder = sagaLogFile.getParentFile();
        String logfileBasename = getLogfileBasename();
        String logfileName = sagaLogFile.getName();
        int n = getHighestArchiveIndex(parentFolder, logfileBasename);
        if (n == -1 && (!sagaLogFile.exists() || sagaLogFile.length() == 0)) {
            return null; // no existing or archived saga-log content
        }
        if (n == -1) {
            n = 0;
        }
        if (!sagaLogFile.exists() || sagaLogFile.length() == 0) {
            File file = new File(parentFolder, logfileBasename + "." + n + ".log");
            return file.toPath(); // only archived log-files, return newest one.
        }
        File archiveToFile = new File(parentFolder, logfileBasename + "." + (n + 1) + ".log");
        try {
            Files.move(sagaLogFile.toPath(), archiveToFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            sagaLogFile = new File(parentFolder, logfileName);
            sagaLogFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException();
        }
        return archiveToFile.toPath(); // rotated-to-file
    }

    private String getLogfileBasename() {
        Pattern logFileNamePattern = Pattern.compile("(.*)\\.log");
        Matcher m = logFileNamePattern.matcher(sagaLogFile.getName());
        if (!m.matches()) {
            throw new IllegalStateException("Saga log file name does not end with .log");
        }
        String logfileBasename = m.group(1);
        return logfileBasename;
    }

    private int getHighestArchiveIndex(File parentFolder, String basename) {
        int n = -1;
        Pattern archivedLogFileNamePattern = Pattern.compile(basename + "\\.([0-9]*)\\.log");
        File[] files = parentFolder.listFiles((dir, name) -> archivedLogFileNamePattern.matcher(name).matches());
        for (File f : files) {
            Matcher m = archivedLogFileNamePattern.matcher(f.getName());
            m.matches();
            int a = Integer.parseInt(m.group(1));
            if (a > n) {
                n = a;
            }
        }
        return n;
    }

    @Override
    public String write(SagaLogEntry entry) {
        synchronized (lock) {
            try {
                if (logWriter == null) {
                    logWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sagaLogFile, true), StandardCharsets.UTF_8));
                }
                logWriter.write(entry.toString());
                logWriter.newLine();
                logWriter.flush();
                if (Saga.ID_END.equals(entry.nodeId)) {
                    openSagas.remove(entry.executionId);
                } else if (Saga.ID_START.equals(entry.nodeId)) {
                    openSagas.add(entry.executionId);
                }
                entries++;
                if (entries >= maxEntriesBeforeRotation) {
                    if (rotationInProgress) {
                        maxEntriesBeforeRotation *= 2;
                        LOG.warn("Recovered log after rotation exceeds limit. {} >= max-limit, limit automatically increased to {}", entries, maxEntriesBeforeRotation);
                    } else {
                        LOG.info("Reached limit. {} >= max-entries, rotating logs", entries);
                        entries = 0;
                        rotationInProgress = true;
                        try {
                            rotateAndRecoverLog();
                        } finally {
                            rotationInProgress = false;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return "{\"logid\":\"" + System.currentTimeMillis() + "\"}";
    }

    @Override
    public List<SagaLogEntry> readEntries(String executionId) {
        List<SagaLogEntry> result = new ArrayList<>();
        synchronized (lock) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sagaLogFile), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    SagaLogEntry entry = SagaLogEntry.from(line);
                    if (executionId.equals(entry.executionId)) {
                        result.add(entry);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    Map<String, Map<String, List<SagaLogEntry>>> readAllIncompleteSagas() {
        Map<String, Map<String, List<SagaLogEntry>>> logEntriesByExecutionId = new LinkedHashMap<>();
        synchronized (lock) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sagaLogFile), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    SagaLogEntry entry = SagaLogEntry.from(line);
                    if (Saga.ID_END.equals(entry.nodeId)) {
                        logEntriesByExecutionId.remove(entry.executionId);
                        continue;
                    }
                    Map<String, List<SagaLogEntry>> entriesByNodeId = logEntriesByExecutionId.get(entry.executionId);
                    if (entriesByNodeId == null) {
                        entriesByNodeId = new LinkedHashMap<>();
                        logEntriesByExecutionId.put(entry.executionId, entriesByNodeId);
                    }
                    List<SagaLogEntry> entries = entriesByNodeId.get(entry.nodeId);
                    if (entries == null) {
                        entries = new ArrayList<>();
                        entriesByNodeId.put(entry.nodeId, entries);
                    }
                    entries.add(entry);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return logEntriesByExecutionId;
    }

    private Map<String, List<SagaLogEntry>> readAllIncompleteSagas(File sagaLogFile) {
        synchronized (lock) {
            Map<String, List<SagaLogEntry>> logEntriesByExecutionId = new TreeMap<>();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sagaLogFile), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    SagaLogEntry entry = SagaLogEntry.from(line);
                    if (Saga.ID_END.equals(entry.nodeId)) {
                        logEntriesByExecutionId.remove(entry.executionId);
                        continue;
                    }
                    List<SagaLogEntry> entries = logEntriesByExecutionId.computeIfAbsent(entry.executionId, eid -> new LinkedList<>());
                    entries.add(entry);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return logEntriesByExecutionId;
        }
    }
}
