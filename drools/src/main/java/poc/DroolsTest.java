package poc;

/**
 * Created by eyallevy on 22/01/17.
 */

import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.*;
import org.drools.io.ResourceFactory;

/* This is a sample class to launch a rule. */

public class DroolsTest {

    public KnowledgeBase readKnowledgeBase() throws Exception {

        KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();

        kbuilder.add(ResourceFactory.newClassPathResource("Pune.drl"), ResourceType.DRL);
        kbuilder.add(ResourceFactory.newClassPathResource("Nagpur.drl"), ResourceType.DRL);

        KnowledgeBuilderErrors errors = kbuilder.getErrors();

        if (errors.size() > 0) {
            for (KnowledgeBuilderError error : errors) {
                System.err.println(error);
            }
            throw new IllegalArgumentException("Could not parse knowledge.");
        }

        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());

        return kbase;
    }
}

