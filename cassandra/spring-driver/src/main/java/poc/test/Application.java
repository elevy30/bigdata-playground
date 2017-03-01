package poc.test;


import poc.model.Person;
import poc.repo.PersonRepo;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;


public class Application {
    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new ClassPathResource("spring-config.xml").getPath());

        PersonRepo personRepo = context.getBean(PersonRepo.class);

        Person personAchilles = new Person();
        personAchilles.setpId(1);
        personAchilles.setName("Achilles");
        personRepo.save(personAchilles);

        Person personHektor = new Person();
        personHektor.setpId(2);
        personHektor.setName("Hektor");
        personRepo.save(personHektor);

        Iterable<Person> personList = personRepo.findAll();
        System.out.println("Person List : ");
        for (Person person : personList) {
            System.out.println(person);
        }

        context.close();

    }
}
