package no.ssb.lds.core.saga;

import no.ssb.saga.execution.sagalog.SagaLogEntry;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class FileSagaLogTest {

    private File cleanState(String logname) {
        File file = new File("target/filesagalogtest", logname + ".log");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        Pattern archivedPattern = Pattern.compile(logname + "\\.[0-9]*\\.log");
        File[] list = file.getParentFile().listFiles((dir, name) -> archivedPattern.matcher(name).matches());
        if (list != null) {
            for (File f : list) {
                f.delete();
            }
        }
        return file;
    }

    private File archive1(String logname) {
        return new File("target/filesagalogtest", logname + ".1.log");
    }

    private File archive2(String logname) {
        return new File("target/filesagalogtest", logname + ".2.log");
    }

    private File archive3(String logname) {
        return new File("target/filesagalogtest", logname + ".3.log");
    }

    @Test
    public void thatNewSagaLogFileIsAutomaticallyCreated() {
        File file = cleanState("autocreate");
        assertFalse(file.exists());
        new FileSagaLog(file, -1).close();
        assertTrue(file.exists());
    }

    @Test
    public void thatLogIsNotRotatedWhenSagaLogFileDoesNotExsists() {
        File file = cleanState("notrotatedifnotexists");
        assertFalse(file.exists());
        new FileSagaLog(file, -1).close();
        assertTrue(file.exists());
        assertFalse(new File("target/tmp/filesagalogtest-notrotatedifnotexists.1.log").exists());
    }

    @Test
    public void thatLogIsNotRotatedWhenSagaLogIsEmpty() throws IOException {
        File file = cleanState("notrotatedifempty");
        file.createNewFile();
        assertEquals(file.length(), 0);
        new FileSagaLog(file, -1).close();
        assertTrue(file.exists());
        assertFalse(new File("target/tmp/filesagalogtest-notrotatedifempty.1.log").exists());
    }

    @Test
    public void thatFirstRotatedFileHasIndex1() throws IOException {
        File file = cleanState("firstindex");
        assertFalse(file.exists());
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            bw.write("e1 Start S FirstRotatedIndexShouldBe1Saga {}\n");
            bw.write("e1 End E\n");
        }
        assertFalse(archive1("firstindex").exists());
        new FileSagaLog(file, -1).close();
        assertTrue(file.exists());
        assertTrue(archive1("firstindex").exists());
    }

    @Test
    public void thatSecondRotatedFileHasIndex2() throws IOException {
        File file = cleanState("secondindex");
        File archive1 = archive1("secondindex");
        File archive2 = archive2("secondindex");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            bw.write("e1 Start S SecondRotatedIndexShouldBe2Saga {}\n");
            bw.write("e1 End E\n");
        }
        archive1.createNewFile();
        assertFalse(archive2.exists());
        new FileSagaLog(file, -1).close();
        assertTrue(archive2.exists());
    }

    @Test
    public void thatSagaLogContainsIncompleteSagasAfterRotation() throws IOException {
        File file = cleanState("incompletesaga");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            bw.write("e1 Start S Complete1 {}\n");
            bw.write("e1 End E\n");
            bw.write("e2 Start S IncompleteSaga1 {}\n");
            bw.write("e3 Start S Complete2 {}\n");
            bw.write("e3 End E\n");
            bw.write("e4 Start S IncompleteSaga2 {}\n");
        }
        FileSagaLog fileSagaLog = new FileSagaLog(file, -1);
        Set<String> openSagaExecutionIds = fileSagaLog.snapshotOpenSagaExecutionIds();
        assertTrue(openSagaExecutionIds.contains("e2"));
        assertTrue(openSagaExecutionIds.contains("e4"));
        fileSagaLog.close();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "e2 Start S IncompleteSaga1 {}");
            assertEquals(br.readLine(), "e4 Start S IncompleteSaga2 {}");
            assertNull(br.readLine());
        }
    }

    @Test
    public void thatIncompleteSagasAreTransferredFromArchiveWhenSagaLogIsMissing() throws IOException {
        File file = cleanState("recoverfromarchive");
        File archive = archive1("recoverfromarchive");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(archive), StandardCharsets.UTF_8))) {
            bw.write("r1 Start S Complete1 {}\n");
            bw.write("r1 End E\n");
            bw.write("r2 Start S IncompleteSaga1 {}\n");
            bw.write("r3 Start S Complete2 {}\n");
            bw.write("r3 End E\n");
            bw.write("r4 Start S IncompleteSaga2 {}\n");
        }
        assertFalse(file.exists());
        FileSagaLog fileSagaLog = new FileSagaLog(file, -1);
        Set<String> openSagaExecutionIds = fileSagaLog.snapshotOpenSagaExecutionIds();
        assertTrue(openSagaExecutionIds.contains("r2"));
        assertTrue(openSagaExecutionIds.contains("r4"));
        fileSagaLog.close();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "r2 Start S IncompleteSaga1 {}");
            assertEquals(br.readLine(), "r4 Start S IncompleteSaga2 {}");
            assertNull(br.readLine());
        }
    }

    @Test
    public void thatSagaLogRotatesAutomaticallyWhenLimitIsExceeded() throws IOException {
        File file = cleanState("rotatewhenlimitexceeded");
        File archive = archive1("rotatewhenlimitexceeded");
        FileSagaLog fileSagaLog = new FileSagaLog(file, 5);
        fileSagaLog.write(SagaLogEntry.startSaga("e1", "name1", "{}"));
        fileSagaLog.write(SagaLogEntry.endSaga("e1"));
        fileSagaLog.write(SagaLogEntry.startSaga("e2", "name2", "{}"));
        fileSagaLog.write(SagaLogEntry.endSaga("e2"));
        fileSagaLog.write(SagaLogEntry.startSaga("e3", "name3", "{}"));
        fileSagaLog.write(SagaLogEntry.endSaga("e3"));
        fileSagaLog.write(SagaLogEntry.startSaga("e4", "name4", "{}"));
        fileSagaLog.write(SagaLogEntry.endSaga("e4"));
        fileSagaLog.close();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(archive), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "e1 Start S name1 {}");
            assertEquals(br.readLine(), "e1 End E");
            assertEquals(br.readLine(), "e2 Start S name2 {}");
            assertEquals(br.readLine(), "e2 End E");
            assertEquals(br.readLine(), "e3 Start S name3 {}");
            assertNull(br.readLine());
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "e3 Start S name3 {}");
            assertEquals(br.readLine(), "e3 End E");
            assertEquals(br.readLine(), "e4 Start S name4 {}");
            assertEquals(br.readLine(), "e4 End E");
            assertNull(br.readLine());
        }
    }

    @Test
    public void thatSagaLogRotationTerminatesNormallyAndDoesNotHaveNPlus1Problem() throws IOException {
        File file = cleanState("normalterminationwhenmisconfigured");
        File archive = archive1("normalterminationwhenmisconfigured");
        File archive2 = archive2("normalterminationwhenmisconfigured");
        File archive3 = archive3("normalterminationwhenmisconfigured");
        FileSagaLog fileSagaLog = new FileSagaLog(file, 3);
        fileSagaLog.write(SagaLogEntry.startSaga("e1", "name1", "{}"));
        fileSagaLog.write(SagaLogEntry.startSaga("e2", "name2", "{}"));
        fileSagaLog.write(SagaLogEntry.startSaga("e3", "name3", "{}"));
        fileSagaLog.write(SagaLogEntry.startSaga("e4", "name4", "{}")); // will potentially reveal N+1 problem
        fileSagaLog.write(SagaLogEntry.startSaga("e5", "name5", "{}")); // will potentially reveal N+1 problem
        fileSagaLog.write(SagaLogEntry.startSaga("e6", "name6", "{}")); // will potentially reveal N+1 problem
        fileSagaLog.write(SagaLogEntry.startSaga("e7", "name7", "{}")); // will potentially reveal N+1 problem
        fileSagaLog.close();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(archive), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "e1 Start S name1 {}");
            assertEquals(br.readLine(), "e2 Start S name2 {}");
            assertEquals(br.readLine(), "e3 Start S name3 {}");
            assertNull(br.readLine());
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(archive2), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "e1 Start S name1 {}");
            assertEquals(br.readLine(), "e2 Start S name2 {}");
            assertEquals(br.readLine(), "e3 Start S name3 {}");
            assertEquals(br.readLine(), "e4 Start S name4 {}");
            assertEquals(br.readLine(), "e5 Start S name5 {}");
            assertEquals(br.readLine(), "e6 Start S name6 {}");
            assertNull(br.readLine());
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            assertEquals(br.readLine(), "e1 Start S name1 {}");
            assertEquals(br.readLine(), "e2 Start S name2 {}");
            assertEquals(br.readLine(), "e3 Start S name3 {}");
            assertEquals(br.readLine(), "e4 Start S name4 {}");
            assertEquals(br.readLine(), "e5 Start S name5 {}");
            assertEquals(br.readLine(), "e6 Start S name6 {}");
            assertEquals(br.readLine(), "e7 Start S name7 {}");
            assertNull(br.readLine());
        }
        assertFalse(archive3.exists(), "Possible (N + 1) problem, more archived logs than expected generated from single rotation.");
    }

    @Test
    public void thatNegativeLimitWillTriggerDefaultLimitUse() {
        File file = cleanState("defaultlimit");
        FileSagaLog fileSagaLog = new FileSagaLog(file, -3);
        assertTrue(fileSagaLog.getMaxEntriesBeforeRotation() > 0);
        fileSagaLog.close();
    }
}
