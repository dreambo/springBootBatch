package ch.javaee.springbootbatch.reader;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.item.database.JpaPagingItemReader;

import ch.javaee.springbootbatch.model.Person;

public class DBReader extends JpaPagingItemReader<Person> {

	public DBReader(EntityManagerFactory emf) throws Exception {
		super();
        setQueryString("select p from Person p where p.familyName like ('%old%')");
        setEntityManagerFactory(emf);
        afterPropertiesSet();
        setPageSize(3);
        setSaveState(true);
	}
}
