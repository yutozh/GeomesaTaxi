package org.zyt.geomesa.GeomesaTools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.zyt.geomesa.Utils.RecordTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GeomesaInsert {
    public static String typeName = "taxi-geomesa";
    private final Map<String, String> params;
    private final TutorialData data;

    public GeomesaInsert(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        this.data = data;
        initializeFromOptions(command);
    }
    public void insertRoute(){
        DataStore datastore = null;
        try {
            datastore = createDataStore(params);

            System.out.println("Cleaning up test data");
            try {
                // delete before insert
                if (datastore instanceof GeoMesaDataStore) {
                    ((GeoMesaDataStore) datastore).delete();
                } else {
                    ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                    datastore.removeSchema(typeName);
                }
                System.out.println("Old table cleaned");
            } catch (TableNotFoundException e){
                System.out.println("Table not exists");
            }

            long time1=System.currentTimeMillis();
            datastore = createDataStore(params);
            SimpleFeatureType sft = getSimpleFeatureType(data);
            createSchema(datastore, sft);
            List<SimpleFeature> features = getFeatures(data);
            writeFeatures(datastore, sft, features);
            long time2=System.currentTimeMillis();
            RecordTime.writeLocalStrOne("Insert "+(time2-time1)+ "\n", "");

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);

        options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());

        return options;
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Creating data store");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public void initializeFromOptions(CommandLine command) {
    }

    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        return data.getSimpleFeatureType();
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        System.out.println();
    }

    public List<SimpleFeature> getFeatures(TutorialData data) {
        System.out.println("Generating data");
        List<SimpleFeature> features = data.getTestData();
        System.out.println();
        return features;
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            System.out.println("Writing data");
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                         datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // WRITE A FEATURE TO HBASE

                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
//                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
//                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
            System.out.println("Wrote " + features.size() + " features");
            System.out.println();
        }
    }
    public static void main(String[] args) throws IOException {
        try {
            args = new String[]{"--hbase.catalog", "a", "--hbase.zookeepers", "192.168.1.133"};
            new GeomesaInsert(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                    .insertRoute();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }

}
