package com.assessment.taskB;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@EnableBatchProcessing
@SpringBootApplication
public class TaskBApplication {

		@Autowired
		private JobBuilderFactory jobBuilderFactory;

		@Autowired
		private StepBuilderFactory stepBuilderFactory;
		
		@Value("${max-threads}")
		private int threadCount;
		
		@Value("${inputResource}")
		private String inputResourcePath;
		
		@Value("${outputResource}")
		private String outputResourcePath;
		
		private Resource outputResource = new FileSystemResource(outputResourcePath);
		private Resource inputFileResource = new FileSystemResource(inputResourcePath);
		
		public DataProcessor getProcessor() {
			return new DataProcessor();
		}
		
		@Bean
		@StepScope
		public FlatFileItemReader<Employee> fileTransactionReader() {
			return new FlatFileItemReaderBuilder<Employee>()
					.name("flatFileEmployeeReader")
					.resource(inputFileResource)
					.delimited()
					.names(new String[] {"name", "place"})
					.fieldSetMapper(fieldSet -> {
						Employee employee = new Employee();

						employee.setName(fieldSet.readString("name"));
						employee.setPlace(fieldSet.readString("place"));
						
						return employee;
					})
					.build();
		}


		@Bean
		@StepScope
		public FlatFileItemWriter<Employee> writer() {
			
			//Create writer instance
	        FlatFileItemWriter<Employee> writer = new FlatFileItemWriter<>();
	         
	        //Set output file location
	        writer.setResource(outputResource);
	         
	        //All job repetitions should "append" to same output file
	        writer.setAppendAllowed(true);
	         
	        //Name field values sequence based on object properties 
	        writer.setLineAggregator(new DelimitedLineAggregator<Employee>() {
	            {
	                setDelimiter(",");
	                setFieldExtractor(new BeanWrapperFieldExtractor<Employee>() {
	                    {
	                        setNames(new String[] { "name", "place" });
	                    }
	                });
	            }
	        });
	        return writer;
			
						
		}

		@Bean
		public Job multithreadedJob() {
			return this.jobBuilderFactory.get("taskB")
					.start(step1())
					.build();
		}

		@Bean
		public Step step1() {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(threadCount);
			taskExecutor.setMaxPoolSize(threadCount);
			taskExecutor.afterPropertiesSet();
			
			
			return this.stepBuilderFactory.get("step1")
					.<Employee, Employee>chunk(100)
					.reader(fileTransactionReader())
					.writer(writer())
					.taskExecutor(taskExecutor)
					.build();
		}
		

		public static void main(String[] args) {
			String [] newArgs = new String[] {};
			
			
			SpringApplication.run(TaskBApplication.class, newArgs);
		}
	}
