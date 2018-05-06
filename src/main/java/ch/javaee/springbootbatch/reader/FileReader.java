package ch.javaee.springbootbatch.reader;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.core.io.Resource;

import ch.javaee.springbootbatch.model.Person;

public class FileReader extends FlatFileItemReader<Person> {

	public FileReader(Resource resource, LineMapper<Person> lineMapper) {
		super();
		setResource(resource);
		setLineMapper(lineMapper);
	}
}
