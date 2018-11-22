package org.zyt.geomesa.GeomesaTools;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.SortByImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseStatsAggregator;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.geotools.data.Transaction;
import org.locationtech.geomesa.index.iterators.StatsScan;
import org.locationtech.geomesa.index.stats.GeoMesaStats;
import org.locationtech.geomesa.utils.stats.Stat;

import org.locationtech.geomesa.utils.stats.StatSerializer;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.zyt.geomesa.Utils.RecordTime;

import javax.measure.unit.SystemOfUnits;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.*;

public class GeomesaSearch {
    public static String typeName = "taxi-geomesa";
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    public static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private final Map<String, String> params;
    private final TutorialData data;

    public GeomesaSearch(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        this.data = data;
        initializeFromOptions(command);
    }
    public ByteArrayOutputStream searchRoute(String startTime, String endTime, String carID,
                                             String startAreaString, String endAreaString, String maxView) throws IOException {
        String queryCQL = getECQL(startTime, endTime, carID, startAreaString, endAreaString, maxView);
//        System.out.println(queryCQL);

        // help to recode time
        String hasSA = "";
        String hasEA = "";
        if(startAreaString != "" && startAreaString != null){
            hasSA = "sa";
        }
        if(endAreaString != "" && endAreaString != null){
            hasEA = "ea";
        }

        ByteArrayOutputStream result = null;
        List<SimpleFeature> queryResult = null;
        DataStore datastore = null;
        try {
            long time1=System.currentTimeMillis();

            datastore = createDataStore(params);
            SimpleFeatureType sft = getSimpleFeatureType(data);
            createSchema(datastore, sft);
            System.out.println(sft.getUserData());

            FilterFactoryImpl ff = new FilterFactoryImpl();
            Query query = new Query(typeName, ECQL.toFilter(queryCQL));
            // set max return features
            if(maxView != null && !"".equals(maxView)){
                query.setMaxFeatures(Integer.parseInt(maxView));
            }
            query.setSortBy(new SortBy[]{new SortByImpl(ff.property("startTime"), SortOrder.ASCENDING)});

            queryResult = queryFeatures(datastore, query);
            // Limit max number result
//            queryResult = queryResult.subList(0, Math.min(queryResult.size(), Integer.parseInt(maxView)));

            result = featuresToByteArray(queryResult);
            long time2=System.currentTimeMillis();
            RecordTime.writeLocalStrOne("Search "+(time2-time1) + " " +
                    startTime  + " " +
                    endTime + " " +
                    carID + " " +
                    hasSA  + " " +
                    hasEA + " " +
                    maxView + " " +
                    queryResult.size() + "\n" , "");
        } catch (CQLException e) {
            throw new RuntimeException("Error creating filter:", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


    public String getECQL(String startTime, String endTime, String carID,
                          String startAreaString, String endAreaString, String maxView) {
        String queryCQL = "";
        if (startTime!=null && !startTime.equals("")) {
            String startTimeF = sdf.format(Long.parseLong(startTime) * 1000 - 28800000).replace("+0800", "");
            queryCQL = queryCQL + "endTime >= '" + startTimeF + "' AND ";
        }
        if (endTime!=null && !endTime.equals("")) {
            String endTimeF = sdf.format(Long.parseLong(endTime) * 1000 - 28800000).replace("+0800", "");
            queryCQL = queryCQL + "startTime <= '" + endTimeF + "' AND ";
        }
        if (carID!=null && !carID.equals("")) {
            queryCQL = queryCQL + "carID = '" + carID + "' AND ";
        }
        if (startAreaString!=null && !startAreaString.equals("")) {
            queryCQL = queryCQL + "CONTAINS(Polygon((" + polygonFormat(startAreaString) + ")), startPoint) AND ";
        }
        if (endAreaString!=null && !endAreaString.equals("")) {
            queryCQL = queryCQL + "CONTAINS(Polygon((" + polygonFormat(endAreaString) + ")), endPoint) AND ";
        }
        queryCQL = queryCQL.substring(0, queryCQL.length() - 4); // remove the last AND
        return queryCQL;
    }

    public String polygonFormat(String s) {
        String res = "";
        StringBuilder resBuilder = new StringBuilder();
        if (s != "" && s != null) {
            String list1[] = s.split(";");
            for (String pointStr : list1) {
                double x = Double.parseDouble(pointStr.split(",")[0]);
                double y = Double.parseDouble(pointStr.split(",")[1]);
                resBuilder.append(y);
                resBuilder.append(" ");
                resBuilder.append(x);
                resBuilder.append(",");
            }
            res = resBuilder.toString();
            res = res.substring(0, res.length() - 1);
        }
        return res;
    }

    public List<SimpleFeature> queryFeatures(DataStore datastore, Query query) throws IOException {
        List<SimpleFeature> queryFeatureList = new ArrayList<>();

        System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
        if (query.getPropertyNames() != null) {
            System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
        }
        if (query.getSortBy() != null) {
            SortBy sort = query.getSortBy()[0];
            System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
        }
        // submit the query, and get back an iterator over matching features
        // use try-with-resources to ensure the reader is closed
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            int n = 0;
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                queryFeatureList.add(feature);
                n++;
            }
            System.out.println();
            System.out.println("Returned " + n + " total features");
            System.out.println();
        }

        return queryFeatureList;
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

    public ByteArrayOutputStream featuresToByteArray(List<SimpleFeature> queryResult) throws IOException, java.text.ParseException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayOutputStream resBaos = new ByteArrayOutputStream();

        resBaos.write(getBytes(queryResult.size()));
        List<String> carIDList = new ArrayList<String>();

        for (SimpleFeature feature : queryResult) {
            String carID = feature.getProperty("carID").getValue().toString();
            String afterLength  = feature.getProperty("afterLength").getValue().toString();
            String avgSpeed = feature.getProperty("avgSpeed").getValue().toString();
            String tpList = feature.getProperty("tpList").getValue().toString();
            String startTime = String.valueOf(((Date) feature.getProperty("startTime").getValue()).getTime() / 1000);
            String endTime = String.valueOf(((Date) feature.getProperty("endTime").getValue()).getTime() / 1000);

            carIDList.add(carID);

            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(startTime))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(endTime))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(avgSpeed))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(afterLength))));

            String[] tpItems = tpList.split(",");

            for (int i=0;i<tpItems.length; i++){
                baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(tpItems[i]))));
            }
        }
        for (String car: carIDList){
            byte[] bytes = car.getBytes();
            resBaos.write(bytes);
        }

        resBaos.write(baos.toByteArray());

        return resBaos;
    }
    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }

    public String getCarNum(){
        DataStore datastore = null;
        String totalNum = "0";
        String carNum = "0";
        String minmaxDate =  "{\"min\": 0, \"max\": 0}";
        try {
            datastore = createDataStore(params);
            SimpleFeatureType sft = getSimpleFeatureType(data);
            createSchema(datastore, sft);

            // get totalNum
            Query query = new Query(typeName);
            query.getHints().put(QueryHints.STATS_STRING(), "Count()");
            Type t1  = new TypeToken<Map<String,String>>(){}.getType();
            Map<String,String> list1 = new Gson().fromJson(startQuery(datastore, query), t1);
            totalNum = list1.get("count");

            // get carNum
            query = new Query(typeName);
            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"carID\",Count())");
            Type t2  = new TypeToken<List<Map<String,Map<String,String>>>>(){}.getType();
            List<Map<String,Map<String,String>>> list2 = new Gson().fromJson(startQuery(datastore, query), t2);
            carNum = String.valueOf(list2.size());

            // get min_maxStartTime
            query = new Query(typeName);
            query.getHints().put(QueryHints.STATS_STRING(), "MinMax(\"startTime\")");
            Type t3  = new TypeToken<Map<String,String>>(){}.getType();
            Map<String,String> list3 = new Gson().fromJson(startQuery(datastore, query), t3);
            String min = String.valueOf(sdf2.parse(list3.get("min")).getTime() / 1000 + 28800);
            String max = String.valueOf(sdf2.parse(list3.get("max")).getTime() / 1000 + 28800);
            minmaxDate = "{\"min\":" + min +", \"max\":" + max +"}";

            return "{\"totalNum\":" + totalNum + ", \"carNum\":" + carNum + ", \"dateInfo\":" + minmaxDate + "}";
        }
        catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
    public String startQuery(DataStore datastore, Query query){
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            String countJson = reader.next().getAttribute(0).toString();

            return countJson;
        }catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) throws IOException {
        try {
            args = new String[]{"--hbase.catalog", "a", "--hbase.zookeepers", "192.168.1.133"};
//            String[] queryArgs = new String[]{"1391097600","1391356800","MMC8000GPSANDASYN051113-24031-00000000",
//                    "30.699998413819344,114.27431088282236;30.66435133685907,114.2250918328665;30.64645588596933,114.16284328303868;30.600641310705427,114.14425454049046;30.55514216961467,114.15615774991699;30.526901799773015,114.21209772585422;30.49581165146344,114.24200154621985;30.48704896697609,114.28111915983226;30.47993151370619,114.31930093802578;30.494334009970245,114.36866576479298;30.522073047341294,114.38442634299878;30.548282230864558,114.38911547800978;30.574826727850024,114.38776149704336;30.598135889247985,114.37489154828143;30.611068468313476,114.35999724045945;30.629526232052896,114.35278738406373;30.65509279752601,114.37182499890577;30.678295584750188,114.35844206679155;30.687458411581588,114.33438982600961;30.677472747372846,114.3137331462425;30.67747274735173,114.31373314628206;30.66772743312417,114.27469060530387;30.699998413819344,114.27431088282236",
//                    "30.699998413819344,114.27431088282236;30.66435133685907,114.2250918328665;30.64645588596933,114.16284328303868;30.600641310705427,114.14425454049046;30.55514216961467,114.15615774991699;30.526901799773015,114.21209772585422;30.49581165146344,114.24200154621985;30.48704896697609,114.28111915983226;30.47993151370619,114.31930093802578;30.494334009970245,114.36866576479298;30.522073047341294,114.38442634299878;30.548282230864558,114.38911547800978;30.574826727850024,114.38776149704336;30.598135889247985,114.37489154828143;30.611068468313476,114.35999724045945;30.629526232052896,114.35278738406373;30.65509279752601,114.37182499890577;30.678295584750188,114.35844206679155;30.687458411581588,114.33438982600961;30.677472747372846,114.3137331462425;30.67747274735173,114.31373314628206;30.66772743312417,114.27469060530387;30.699998413819344,114.27431088282236"};
            String[] queryArgs = new String[]{"1394294400","1394553600","MMC8000GPSANDASYN051113-24031-00000000",
                    "30.63530499158118,114.2747493313646;30.603159551261527,114.22370746587717;30.5657007520701,114.2181404891204;30.53724232418722,114.25797708273006;30.572577763852564,114.28439504838872;30.614571204282683,114.31175739238586;30.63530499158118,114.2747493313646",
                    "30.63530499158118,114.2747493313646;30.603159551261527,114.22370746587717;30.5657007520701,114.2181404891204;30.53724232418722,114.25797708273006;30.572577763852564,114.28439504838872;30.614571204282683,114.31175739238586;30.63530499158118,114.2747493313646"
            };

            int[] times = new int[]{100,2500,5000,7500,10000};
            String[] endtime = new String[]{"1394553600","1394899200","1395590400","1396972800","1402243200"};
//            for (int i=0;i<=4;i++){
//                for(int j=0;j<=4;j++){
//                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                            .searchRoute(queryArgs[0],endtime[i],"","","",String.valueOf(times[j]));
//                    System.out.print("=");
//                }
//            }
//            System.out.println();
//            for (int i=0;i<=4;i++){
//                for(int j=0;j<4;j++){
//                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                            .searchRoute("",queryArgs[1],"","","",String.valueOf(times[i]));
//                    System.out.print("=");
//                }
//            }
//            for (int i=0;i<=4;i++){
//                for(int j=0;j<4;j++){
//                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                            .searchRoute("","",queryArgs[2],"","",String.valueOf(times[i]));
//                    System.out.print("=");
//                }
//            }
//            System.out.println();
//            for (int i=0;i<=4;i++){
//                for(int j=0;j<4;j++){
//                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                            .searchRoute("","","",queryArgs[3],"",String.valueOf(times[i]));
//                    System.out.print("=");
//                }
//            }
//            System.out.println();
//            for (int i=0;i<=4;i++){
//                for(int j=0;j<4;j++){
//                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                            .searchRoute("","","","",queryArgs[4],String.valueOf(times[i]));
//                    System.out.print("=");
//                }
//            }
//            System.out.println();
//            for (int i=0;i<=4;i++){
//                for(int j=0;j<4;j++){
//                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                            .searchRoute("","","",queryArgs[3],queryArgs[4],String.valueOf(times[i]));
//                    System.out.print("=");
//                }
//            }
//            System.out.println();
            for (int i=0;i<=4;i++){
                for(int j=0;j<4;j++){
                    new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                    .searchRoute(queryArgs[0],queryArgs[1],"",queryArgs[3],queryArgs[4],String.valueOf(times[i]));
                    System.out.print("=");
                }
            }
//
//            System.out.println();


//            new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                    .searchRoute(queryArgs[0],queryArgs[1],queryArgs[2],queryArgs[3],queryArgs[4],queryArgs[5]);
//
            new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                    .searchRoute("1393603200", "",
                            null,"",
//                            "30.710,114.264;30.710,114.266;30.712,114.266;30.712,114.264;30.710,114.264",
                            null,
                            "10");


//            System.out.println( new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
//                    .getCarNum());
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
