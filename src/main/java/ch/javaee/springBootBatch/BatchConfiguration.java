package ch.javaee.springBootBatch;

import ch.javaee.springBootBatch.job.flow.GroupFlow;
import ch.javaee.springBootBatch.job.flow.GroupFlowBuilder;
import ch.javaee.springBootBatch.model.Person;
import ch.javaee.springBootBatch.processor.PersonItemProcessor;
import ch.javaee.springBootBatch.tokenizer.PersonFixedLengthTokenizer;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowStep;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.job.flow.support.state.FlowState;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


@Configuration
@EnableBatchProcessing
@ComponentScan
//spring boot configuration
@EnableAutoConfiguration
// file that contains the properties
@PropertySource("classpath:application.properties")
public class BatchConfiguration {

    /*
        Load the properties
     */
    @Value("${database.driver}")
    private String databaseDriver;
    @Value("${database.url}")
    private String databaseUrl;
    @Value("${database.username}")
    private String databaseUsername;
    @Value("${database.password}")
    private String databasePassword;


    /**
     * We define a bean that read each line of the input file.
     *
     * @return
     */
    @Bean
    public ItemReader<Person> reader() {
        // we read a flat file that will be used to fill a Person object
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        // we pass as parameter the flat file directory
        reader.setResource(new ClassPathResource("PersonData.txt"));
        // we use a default line mapper to assign the content of each line to the Person object
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            // we use a custom fixed line tokenizer
            setLineTokenizer(new PersonFixedLengthTokenizer());
            // as field mapper we use the name of the fields in the Person class
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                // we create an object Person
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    /**
     * The ItemProcessor is called after a new line is read and it allows the developer
     * to transform the data read
     * In our example it simply return the original object
     *
     * @return
     */
    @Bean
    public ItemProcessor<Person, Person> processor() {
        return new PersonItemProcessor();
    }

    /**
     * Nothing special here a simple JpaItemWriter
     *
     * @return
     */
    @Bean
    public ItemWriter<Person> writer() {
        JpaItemWriter writer = new JpaItemWriter<Person>();
        writer.setEntityManagerFactory(entityManagerFactory().getObject());

        return writer;
    }

    /**
     * This method declare the steps that the batch has to follow
     *
     * @param jobs
     * @return
     */
    @Bean
    public Job flowJobDemo(JobBuilderFactory jobs, StepBuilderFactory stepBuilderFactory, JobRepository jobRepository) {

        Job job = jobs.get("flowJobDemo")
                .incrementer(new RunIdIncrementer()) // because a spring config bug, this incrementer is not really useful
                .flow(precheckFlowStep(stepBuilderFactory, jobRepository))
                .next(validationFlowStep(stepBuilderFactory, jobRepository))
                .next(action1(stepBuilderFactory))
                .next(action2(stepBuilderFactory))
                .next(action3(stepBuilderFactory))
                .next(validationFlowStep_next(stepBuilderFactory, jobRepository))
                .end()
                .listener(new TestJobListener())
                .build();

        return job;
    }

    public Step precheckFlowStep(StepBuilderFactory stepBuilderFactory, JobRepository jobRepository) {
        FlowStep flowStep = new FlowStep();
        flowStep.setName("precheckFlowStep");
        flowStep.setFlow(flowPrecheck(stepBuilderFactory));
        flowStep.setJobRepository(jobRepository);
        return flowStep;
    }

    public Step validationFlowStep(StepBuilderFactory stepBuilderFactory, JobRepository jobRepository) {
        FlowStep flowStep = new FlowStep();
        flowStep.setName("validationFlowStep");
        flowStep.setFlow(flowValidation(stepBuilderFactory));
        flowStep.setJobRepository(jobRepository);
        return flowStep;
    }

    public Step validationFlowStep_next(StepBuilderFactory stepBuilderFactory, JobRepository jobRepository) {
        FlowStep flowStep = new FlowStep();
        flowStep.setName("validationFlowStep_next");
        flowStep.setFlow(flowValidation_next(stepBuilderFactory));
        flowStep.setJobRepository(jobRepository);
        return flowStep;
    }

    @Bean
    public Flow flowValidation(StepBuilderFactory stepBuilderFactory) {
        return new GroupFlowBuilder<GroupFlow>("flowStepValidation")
                .start(validationStep1(stepBuilderFactory))
                .next((validationStep2(stepBuilderFactory)))
                .next(validationStep3(stepBuilderFactory))
                .build();
    }

    @Bean
    public Flow flowPrecheck(StepBuilderFactory stepBuilderFactory) {
        return new GroupFlowBuilder<GroupFlow>("precheckFlow")
                .start(precheckStep1(stepBuilderFactory))
                .next(precheckStep2(stepBuilderFactory))
                .next(precheckStep3(stepBuilderFactory))
                .build();
    }

    @Bean
    public Flow flowValidation_next(StepBuilderFactory stepBuilderFactory) {
        return new GroupFlowBuilder<GroupFlow>("flowStepValidation_next")
                .start(validationStep11(stepBuilderFactory))
                .next((validationStep22(stepBuilderFactory)))
                .next(validationStep33(stepBuilderFactory))
                .build();
    }


    public Step validationStep1(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("validationStep1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("validationStep1");
                return RepeatStatus.FINISHED;
//                throw new RuntimeException("from validationStep1");
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step validationStep2(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("validationStep2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("validationStep2");
                return RepeatStatus.FINISHED;
//                throw new RuntimeException("from validationStep2");
            }
        }).build();

        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step validationStep3(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("validationStep3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("validationStep3");
                return RepeatStatus.FINISHED;
//                throw new RuntimeException("from validationStep3");
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step precheckStep1(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("precheckStep1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("precheckStep1");
                return RepeatStatus.FINISHED;
//                throw new RuntimeException("Test throw from precheckStep1");
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step precheckStep2(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("precheckStep2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("precheckStep2");
//                return RepeatStatus.FINISHED;
                throw new RuntimeException("Test throw from precheckStep2");
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step precheckStep3(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("precheckStep3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("precheckStep3");
                return RepeatStatus.FINISHED;
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    @Bean
    public Step action1(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("action1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("action1");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    public Step action2(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("action2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("action2");
//                throw new RuntimeException("from action2");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    public Step action3(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("action3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("action3");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    public Step validationStep11(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("validationStep11").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("validationStep11");
                return RepeatStatus.FINISHED;
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step validationStep22(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("validationStep22").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("validationStep22");
                return RepeatStatus.FINISHED;
//                throw new RuntimeException("from validationStep22");
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    public Step validationStep33(StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("validationStep33").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("validationStep33");
                return RepeatStatus.FINISHED;
            }
        }).build();
        ((TaskletStep) step).setAllowStartIfComplete(true);
        return step;
    }

    class TestJobListener implements JobExecutionListener {
        @Override
        public void beforeJob(JobExecution jobExecution) {

        }

        @Override
        public void afterJob(JobExecution jobExecution) {
        }
    }

    /**
     * Step
     * We declare that every 1000 lines processed the data has to be committed
     *
     * @param stepBuilderFactory
     * @param reader
     * @param writer
     * @param processor
     * @return
     */

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
                      ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(1000)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    /**
     * As data source we use an external database
     *
     * @return
     */
    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(databaseDriver);
        dataSource.setUrl(databaseUrl);
        dataSource.setUsername(databaseUsername);
        dataSource.setPassword(databasePassword);
        return dataSource;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {

        LocalContainerEntityManagerFactoryBean lef = new LocalContainerEntityManagerFactoryBean();
        lef.setPackagesToScan("ch.javaee.springBootBatch");
        lef.setDataSource(dataSource());
        lef.setJpaVendorAdapter(jpaVendorAdapter());
        lef.setJpaProperties(new Properties());
        return lef;
    }

    @Bean
    public JpaVendorAdapter jpaVendorAdapter() {
        HibernateJpaVendorAdapter jpaVendorAdapter = new HibernateJpaVendorAdapter();
        jpaVendorAdapter.setDatabase(Database.MYSQL);
        jpaVendorAdapter.setGenerateDdl(true);
        jpaVendorAdapter.setShowSql(false);

        jpaVendorAdapter.setDatabasePlatform("org.hibernate.dialect.MySQLDialect");
        return jpaVendorAdapter;
    }

}
