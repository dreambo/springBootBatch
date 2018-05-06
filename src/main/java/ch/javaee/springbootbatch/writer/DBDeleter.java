package ch.javaee.springbootbatch.writer;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.springframework.batch.item.database.JpaItemWriter;

import ch.javaee.springbootbatch.model.Person;

public class DBDeleter extends JpaItemWriter<Person> {

	public DBDeleter(EntityManagerFactory emf) {
		super();
		setEntityManagerFactory(emf);
	}

	@Override
	public void doWrite(EntityManager em, List<? extends Person> persons) {
		for (Person person : persons) {
			em.remove(em.contains(person) ? person : em.merge(person));
		}
	}
}
