package ch.javaee.springbootbatch.processor;

import ch.javaee.springbootbatch.model.Person;
import org.springframework.batch.item.ItemProcessor;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {
    @Override
    public Person process(final Person person) throws Exception {

    	person.setFamilyName(person.getFamilyName() + "_old");
        return person;
    }
}