package poc.repo;

import org.springframework.data.repository.CrudRepository;

import poc.model.Person;

public interface PersonRepo extends CrudRepository<Person, String>
{

}
