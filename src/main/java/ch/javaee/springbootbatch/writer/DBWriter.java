package ch.javaee.springbootbatch.writer;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.springframework.batch.item.database.JpaItemWriter;

import ch.javaee.springbootbatch.model.Person;

public class DBWriter extends JpaItemWriter<Person> {

	public DBWriter(EntityManagerFactory emf) {
		super();
		setEntityManagerFactory(emf);
	}

	@Override
	public void doWrite(EntityManager em, List<? extends Person> persons) {
		super.doWrite(em, persons);
	}
}
