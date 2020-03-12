package es.us.idea.dmn4spark.dmn;

import org.camunda.bpm.dmn.engine.DmnDecision;
import org.camunda.bpm.dmn.engine.DmnDecisionTableResult;
import org.camunda.bpm.dmn.engine.DmnEngine;
import org.camunda.bpm.dmn.engine.DmnEngineConfiguration;
import org.camunda.bpm.dmn.engine.impl.DefaultDmnEngineConfiguration;
import org.camunda.feel.integration.CamundaFeelEngineFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String args[]) throws FileNotFoundException {
        System.out.println("Hello world");

        // configure and build the DMN engine

        //DmnEngine dmnEngine = DmnEngineConfiguration.createDefaultDmnEngineConfiguration().buildEngine();
        DefaultDmnEngineConfiguration dmnEngineConfig = (DefaultDmnEngineConfiguration) DmnEngineConfiguration.createDefaultDmnEngineConfiguration();
        dmnEngineConfig.setFeelEngineFactory(new CamundaFeelEngineFactory());
        dmnEngineConfig.setDefaultOutputEntryExpressionLanguage("feel");
        DmnEngine dmnEngine = dmnEngineConfig.buildEngine();


        InputStream is = new FileInputStream("models/simulation(7).dmn");

        // parse a decision
        DmnDecision decision = dmnEngine.parseDecision("dq", is);
        //List<DmnDecision> decisions = dmnEngine.parseDecisions(is);



        Map<String, Object> data = new HashMap<String, Object>();
        data.put("$BR3", true);
        data.put("$BR4", false);
        data.put("$BR5", true);

        data.put("$BR9", true);
        data.put("$BR10", true);
        data.put("$BR11", true);

        // evaluate a decision
        DmnDecisionTableResult result = dmnEngine.evaluateDecisionTable(decision, data);

        String r = result.getFirstResult().getEntry("dq");

        System.out.println(result);
        System.out.println(r);

    }

}
