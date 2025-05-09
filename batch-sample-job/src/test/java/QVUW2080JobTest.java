import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = "spring.batch.job.name=QVUWDC_20800", classes = com.bzbatch.sample.QVUW2080JobApplication.class)
@SpringBatchTest
@ActiveProfiles({"mock", "qvuwtest"})
class QVUW2080JobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void testJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("ODATE", "20250501")
                .addString("TIME", "1500")
                .addLong("unique", System.currentTimeMillis()) // 추가
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        Assertions.assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }
}