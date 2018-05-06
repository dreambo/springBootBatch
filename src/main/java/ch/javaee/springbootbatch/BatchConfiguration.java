package ch.javaee.springbootbatch;

import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.LineTokenizer;
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

import ch.javaee.springbootbatch.model.Person;
import ch.javaee.springbootbatch.processor.PersonItemProcessor;
import ch.javaee.springbootbatch.reader.DBReader;
import ch.javaee.springbootbatch.reader.FileReader;
import ch.javaee.springbootbatch.tokenizer.PersonFixedLengthTokenizer;
import ch.javaee.springbootbatch.writer.DBDeleter;
import ch.javaee.springbootbatch.writer.DBWriter;


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
    @Value("${spring.datasource.driver}")
    private String databaseDriver;
    @Value("${spring.datasource.url}")
    private String databaseUrl;
    @Value("${spring.datasource.username}")
    private String databaseUsername;
    @Value("${spring.datasource.password}")
    private String databasePassword;


    /**
     * We define a bean that read each line of the input file.
     *
     * @return
     * @throws Exception 
     */
    @Bean
    public ItemReader<Person> fileReader(DataSource dataSource) throws Exception {
    	
        // we use a default line mapper to assign the content of each line to the Person object
    	LineTokenizer lineTokenizer = new PersonFixedLengthTokenizer();
        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<Person>();
        fieldSetMapper.setTargetType(Person.class);

        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<Person>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
    	FileReader reader = new FileReader(new ClassPathResource("PersonData.txt"), lineMapper);

        return reader;
    }

    @Bean
    public ItemReader<Person> dbReader(DataSource dataSource) throws Exception {
    	
    	DBReader reader = new DBReader(entityManagerFactory(dataSource).getObject());

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
     * @return
     */
    @Bean
    public ItemWriter<Person> writer(DataSource dataSource) {
    	
        return new DBWriter(entityManagerFactory(dataSource).getObject());
    }

    /**
     * Nothing special here a simple JpaItemWriter
     * @return
     */
    @Bean
    public ItemWriter<Person> deleter(DataSource dataSource) {
    	
        return new DBDeleter(entityManagerFactory(dataSource).getObject());
    }

    /**
     * This method declare the steps that the batch has to follow
     *
     * @param jobs
     * @param s1
     * @return
     */
    @Bean
    public Job importPerson(JobBuilderFactory jobs, Step readStep, Step deleteStep) {

        return jobs.get("import")
                .incrementer(new RunIdIncrementer()) // because a spring config bug, this incrementer is not really useful
                .flow(readStep)
                .next(deleteStep)
                .end()
                .build();
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
    public Step readStep(StepBuilderFactory stepBuilderFactory, ItemReader<Person> fileReader, ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {

    	return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(1000)
                .reader(fileReader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Step deleteStep(StepBuilderFactory stepBuilderFactory, ItemReader<Person> dbReader, ItemWriter<Person> deleter, ItemProcessor<Person, Person> processor) {

    	return stepBuilderFactory.get("step2")
                .<Person, Person>chunk(1000)
                .reader(dbReader)
                .processor(processor)
                .writer(deleter)
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
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {

        LocalContainerEntityManagerFactoryBean lemf = new LocalContainerEntityManagerFactoryBean();
        lemf.setPackagesToScan("ch.javaee.springbootbatch");
        lemf.setDataSource(dataSource);
        lemf.setJpaVendorAdapter(jpaVendorAdapter());
        lemf.setJpaProperties(new Properties());
        return lemf;
    }


    @Bean
    public JpaVendorAdapter jpaVendorAdapter() {
        HibernateJpaVendorAdapter jpaVendorAdapter = new HibernateJpaVendorAdapter();
        jpaVendorAdapter.setDatabase(Database.POSTGRESQL);
        jpaVendorAdapter.setGenerateDdl(true);
        jpaVendorAdapter.setShowSql(false);
        jpaVendorAdapter.setDatabasePlatform(org.hibernate.dialect.PostgreSQL94Dialect.class.getName());
        return jpaVendorAdapter;
    }
}
