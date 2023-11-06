package com.example.spBatchWriteToFile.Config;

import com.example.spBatchWriteToFile.Models.Customer;
import com.example.spBatchWriteToFile.Repository.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Configuration
public class springBatchConfig {
    private JobRepository jobRepository;
    private CustomerRepository customerRepository;

    private PlatformTransactionManager platformTransactionManager;

    public springBatchConfig(JobRepository jobRepository, CustomerRepository customerRepository,
                             PlatformTransactionManager platformTransactionManager) {
        this.jobRepository = jobRepository;
        this.customerRepository = customerRepository;
        this.platformTransactionManager = platformTransactionManager;
    }

    @Bean
    public Job runJob(){
        return new JobBuilder("customerDataExport", jobRepository)
                .flow(step1()).end()
                .build();
    }

    @Bean
    public Step step1() {
        return new StepBuilder("customerDataExtractStep", jobRepository)
                .allowStartIfComplete(true)
                .<Customer, Customer>chunk(10, platformTransactionManager)
                .reader(customerReaderFromDb())
                .processor(customerItemProcessor())
                .writer(writeToFile())
                .build();
    }

    @Bean
    public FlatFileItemWriter<Customer> writeToFile() {
        FlatFileItemWriter<Customer> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setResource(new FileSystemResource("Customer.txt"));
        flatFileItemWriter.setLineAggregator(lineAggregator());
        flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("ID,First Name,Last Name,Email ID,Gender,Phone,Country,DOB");
            }
        });

        return flatFileItemWriter;
    }

    @Bean
    public LineAggregator<Customer> lineAggregator() {
        DelimitedLineAggregator<Customer> delimitedLineAggregator = new DelimitedLineAggregator<>();
        delimitedLineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<Customer> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
        beanWrapperFieldExtractor.setNames(new String[]{"id", "firstName", "lastName", "email","gender", "contactNo"
        , "country", "dob"});
        delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);

        return delimitedLineAggregator;
    }

    @Bean
    public ItemProcessor<? super Customer,? extends Customer> customerItemProcessor() {
       ItemProcessor<Customer,Customer> itemProcessor = new ItemProcessor<Customer, Customer>() {
           @Override
           public Customer process(Customer item) throws Exception {
               return item;
           }
       };
       return itemProcessor;
    }

    @Bean
    public RepositoryItemReader<Customer> customerReaderFromDb(){
        RepositoryItemReader<Customer> repositoryItemReader = new RepositoryItemReader<>();
        repositoryItemReader.setRepository(customerRepository);
        repositoryItemReader.setMethodName("findAll");
        repositoryItemReader.setPageSize(25);

        HashMap<String, Sort.Direction> sorts = new HashMap<>();
        sorts.put("id", Sort.Direction.ASC);
        repositoryItemReader.setSort(sorts);
        return  repositoryItemReader;
    }
}
