/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.topology.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDFS;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nico
 */
public class LocationCrawlerBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Model dBpediaToLinkedGeoDataMap;
    private HashMap<String, HashMap<String, Integer>> propertyProbabilityMap;
    private boolean initialized = false;
    private final String strExampleEmitFieldsId;
    private final String restletURL;

    public LocationCrawlerBolt(String strExampleEmitFieldsId, String restletURL) {
        super();
        
        this.restletURL = restletURL;
        this.strExampleEmitFieldsId = strExampleEmitFieldsId;
    }

    /**
     * Prepare method is similar the "Open" method for Spouts and is called when
     * a worker is about to be put to work. This method also initialise the main
     * example Storm Java bolt.
     *
     * @param stormConf map of the storm configuration (passed within Storm
     * topology itself, not be a user)
     * @param context context (e.g. similar to description) of the topology
     * (passed within Storm topology itself, not be a user)
     * @param collector output collector of the Storm (which is responsible to
     * emiting new tuples, passed within Storm topology itself, not be a user)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        if (!initialized) {
            init();
            initialized = true;
        }
    }

    private void init() {
        // read the LinkedGeoData <-> DBPedia links file
        dBpediaToLinkedGeoDataMap = ModelFactory.createDefaultModel().read(restletURL + "/static/linkedgeodata_links.nt");

        // read the property probabilites file
        propertyProbabilityMap = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new URL(restletURL + "/static/nb_count.csv").openStream()))) {
            String line;
            String[] lineArray;

            br.readLine();  // skip header line

            while ((line = br.readLine()) != null) {
                lineArray = line.split("\t");
                HashMap<String, Integer> countsMap = new HashMap<String, Integer>();
                countsMap.put("rel", Integer.parseInt(lineArray[1]));
                countsMap.put("irrel", Integer.parseInt(lineArray[2]));
                propertyProbabilityMap.put(lineArray[0], countsMap);
            }
        } catch (IOException ex) {
            Logger.getLogger(LocationCrawlerBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String mapToDBPedia(String linkedGeoDataUri) {
        Model result = dBpediaToLinkedGeoDataMap.query(new SimpleSelector(null, OWL.sameAs, (RDFNode) new ResourceImpl(linkedGeoDataUri)));
        return (result.size() >= 1) ? result.listSubjects().next().getURI() : null;
    }

    private Map<String, ArrayList<String>> lookUpDBPediaUri(String dbPediaUri) {
        Map<String, ArrayList<String>> resultMap = new HashMap<>();
        ParameterizedSparqlString queryString = new ParameterizedSparqlString(
                "SELECT ?prop ?place"
                + " WHERE { ?uri ?prop ?place .}");
        queryString.setIri("?uri", dbPediaUri);

        Query query = QueryFactory.create(queryString.asQuery());
        ResultSet results;
        try (QueryExecution qexec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query)) {
            results = qexec.execSelect();
            while (results.hasNext()) {
                QuerySolution tuple = results.next();
                if (tuple.get("place").isURIResource()) {
                    if (resultMap.get(tuple.get("place").toString()) == null) {
                        resultMap.put(tuple.get("place").toString(), new ArrayList<String>());
                    }
                    resultMap.get(tuple.get("place").toString()).add(tuple.get("prop").toString());
                }
            }
        }

        return resultMap;
    }

    private boolean checkCandidateBasedOnProperties(ArrayList<String> value) {
        boolean probabilityInfoAvailable = false;
        int totalRelevant = propertyProbabilityMap.get("total").get("rel");
        int totalIrrelevant = propertyProbabilityMap.get("total").get("irrel");

        double posApriori = totalRelevant / (double) (totalRelevant + totalIrrelevant);
        double negApriori = totalIrrelevant / (double) (totalRelevant + totalIrrelevant);

        double posOdds = 1;
        double negOdds = 1;

        for (String property : value) {
            if (propertyProbabilityMap.get(property) != null) {
                posOdds *= ((propertyProbabilityMap.get(property).get("rel") + 1) / (double) (totalRelevant + 2)) * posApriori;
                negOdds *= ((propertyProbabilityMap.get(property).get("irrel") + 1) / (double) (totalIrrelevant + 2)) * negApriori;
                probabilityInfoAvailable = true;
            }
        }

        if (!probabilityInfoAvailable) {
            return false;
        } else {
            return (posOdds > negOdds);
        }
    }

    private Map<String, Literal> dereferenceLocation(String locationUri) {
        HashMap<String, Literal> resultMap = new HashMap<>();
        Model locationTriples = ModelFactory.createDefaultModel().read(locationUri);

        ParameterizedSparqlString queryString = new ParameterizedSparqlString(
                "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>"
                + "SELECT ?lat ?long ?label WHERE {"
                + "  ?s geo:lat ?lat ."
                + "  ?s geo:long ?long ."
                + "  OPTIONAL { ?s ?rdfs_label ?label ."
                + "     FILTER LANGMATCHES(LANG(?label), \"en\") }"
                + " }");
        queryString.setIri("?s", locationUri);
        queryString.setIri("?rdfs_label", RDFS.label.getURI());

        Query query = QueryFactory.create(queryString.asQuery());
        ResultSet results;
        try (QueryExecution qexec = QueryExecutionFactory.create(query, locationTriples)) {
            results = qexec.execSelect();
            if (results.hasNext()) {
                QuerySolution result = results.next();
                resultMap.put("uri", (Literal) locationTriples.createLiteral(locationUri));
                resultMap.put("lat", (Literal) result.get("?lat"));
                resultMap.put("long", (Literal) result.get("?long"));
                resultMap.put("label", (Literal) result.get("?label"));
                return resultMap;
            } else {
                return null;
            }
        }
    }

    /**
     * searches for locations in the message and computes related locations
     *
     * @param input standard Storm tuple input object (passed within Storm
     * topology itself, not be a user)
     */
    @Override
    public void execute(Tuple input) {
        // Retrieve hash map tuple object from Tuple input at index 0, index 1 will be message delivery tag (not used here)
        Map<Object, Object> inputMap = (HashMap<Object, Object>) input.getValue(0);
        // Get JSON object from the HashMap from the Collections.singletonList
        Map<Object, Object> message = (Map<Object, Object>) inputMap.get("message");
        collector.ack(input);

        ArrayList<Map<String, Literal>> relatedLocations = new ArrayList<>();

        if (message.containsKey("itinno:loc_set")) {
            List<Object> locationSet = (List<Object>) message.get("itinno:loc_set");

            for (int i = 0; i < locationSet.size(); i++) {
                Map<Object, Object> locationDictionary = (Map<Object, Object>) locationSet.get(i);
                List<Object> linkedDataUris = (List<Object>) locationDictionary.get("linked_data");

                for (Object o : linkedDataUris.toArray()) {
                    String linkedGeoDataUri = (String) o;

                    String dbPediaUri = mapToDBPedia(linkedGeoDataUri);
                    if (dbPediaUri != null) {
                        Map<String, ArrayList<String>> possibleLocations = lookUpDBPediaUri(dbPediaUri);
                        for (Entry<String, ArrayList<String>> e : possibleLocations.entrySet()) {
                            if (checkCandidateBasedOnProperties(e.getValue())) {
                                Map<String, Literal> locationWithCoordinates = dereferenceLocation(e.getKey());
                                if (locationWithCoordinates != null) {
                                    relatedLocations.add(locationWithCoordinates);
                                }
                            }
                        }
                    }
                }
//                JSONArray containerArray = (JSONArray) locationSet.get(i);
//                String linkedGeoDataUri = (String) ((JSONArray) containerArray.get(10)).get(0);
//
//                String dbPediaUri = mapToDBPedia(linkedGeoDataUri);
//                if (dbPediaUri != null) {
//                    Map<String, ArrayList<String>> possibleLocations = lookUpDBPediaUri(dbPediaUri);
//                    for (Entry<String, ArrayList<String>> e : possibleLocations.entrySet()) {
//                        if (checkCandidateBasedOnProperties(e.getValue())) {
//                            Map<String, Literal> locationWithCoordinates = dereferenceLocation(e.getKey());
//                            if (locationWithCoordinates != null) {
//                                relatedLocations.add(locationWithCoordinates);
//                            }
//                        }
//                    }
//                }
            }
        }

        Map<String, Object> geospatialContext = new HashMap<>();
        geospatialContext.put("itinno:item_id", message.get("itinno:item_id"));
        List<Object> exploredEntities = new ArrayList<>();

        for (Map<String, Literal> location : relatedLocations) {
            Map<String, Object> linkedDataInfo = new HashMap<>();
            linkedDataInfo.put("ukob:explored_entity_uri", location.get("uri").getString());
            linkedDataInfo.put("ukob:explored_entity_label", (location.get("label") == null) ? "" : location.get("label").getString());
            String openGisPoint = "POINT(" + location.get("lat").getDouble() + " " + location.get("long").getDouble() + ")";
            linkedDataInfo.put("ukob:explored_entity_loc", openGisPoint);
            exploredEntities.add(linkedDataInfo);
        }

        geospatialContext.put("ukob:explored_entities", exploredEntities);

        Logger.getLogger(LocationCrawlerBolt.class.getName()).log(Level.INFO, "final result= " + geospatialContext.toString());

        this.collector.emit(new Values(geospatialContext));
    }

    /**
     * Declare output field name (in this case simple a string value that is
     * defined in the constructor call)
     *
     * @param declarer standard Storm output fields declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(strExampleEmitFieldsId));
    }
}