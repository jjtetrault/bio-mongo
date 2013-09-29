package com.bluefoundry.pilot.vcfrepo.vcftojson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Jason T
 * Date: 9/18/13
 * Time: 10:41 PM
 *
 */
public class VCFProcessor
{

    // Public Collection Names
    private static final String PUBLIC_COLLECTION = "publicvariants";
    private static final String PUBLIC_FILEMETA = "publicvcf";

    // Standard Collection Names
    private static final String STANDARD_COLLECTION = "variants";
    private static final String STANDARD_FILEMETA = "vcf";

    private String inFile;

    private Map<String, Object> fileHeaders;
    private DB mongoDB;
    private String variantCollection;
    private String vcfCollection;

    private static int flushsize = 10000;

    UUID id = UUID.randomUUID();

    public VCFProcessor(String vcfFile, DB db, String variantCollection, String vcfCollection)
    {
        this.inFile = vcfFile;
        fileHeaders = new HashMap<String, Object>();
        this.mongoDB = db;
        this.vcfCollection = vcfCollection;
        this.variantCollection = variantCollection;
    }

    public void process() throws Exception
    {

        String[] currentHeaders = null;
        BufferedReader breader = this.fileToBufferedReader(inFile);
        String line = breader.readLine();
        VCFLineProcessor processor = null;
        List<DBObject> variantBuffer = new LinkedList<DBObject>();

        while (line != null)
        {
            if (line.startsWith("##"))
            {
                processFileHeaders(line);
            }
            else if (line.startsWith("#"))
            {
                // create or re-create the headers
                String headerline = line.substring(1);
                StringTokenizer tokenizer = new StringTokenizer(headerline);
                ArrayList<String> tokens = new ArrayList<String>();
                while (tokenizer.hasMoreElements())
                {
                    tokens.add(tokenizer.nextToken());
                }
                currentHeaders = tokens.toArray(new String[tokens.size()]);
                // reset the processor
                processor = null;
            }
            else
            {
                if (processor == null)
                {
                    processor = new VCFLineProcessor(currentHeaders, id.toString());
                }

                Map<String, Object> variant;
                variant = processor.processLine(line);
                Gson doc = new GsonBuilder().create();
                String jsonString = doc.toJson(variant);
                DBObject dbObject = (DBObject) JSON.parse(jsonString);

                variantBuffer.add(dbObject);

                if (variantBuffer.size() == flushsize)
                {
                    this.flushBuffer(variantBuffer);
                    variantBuffer.clear();
                }

            }

            line = breader.readLine();

        }
        // do a final flush of the buffer.
        this.flushBuffer(variantBuffer);

        Gson doc = new GsonBuilder().create();
        this.fileHeaders.put("__vcfid", id.toString());
        String fileJSON = doc.toJson(this.fileHeaders);


        DBObject dbObject = (DBObject) JSON.parse(fileJSON);
        DBCollection collection = mongoDB.getCollection(this.vcfCollection);
        collection.insert(dbObject);

        //TODO Ensure Indices

    }

    private void flushBuffer(List<DBObject> variants)
    {
        DBCollection collection = mongoDB.getCollection(variantCollection);
        collection.insert(variants);
        System.out.println("Flushed Buffer");
    }

    private void processFileHeaders(String headerLine)
    {
        headerLine = headerLine.substring(2);
        //TODO Interpret INFO, FILTER and FORMAT
        int first = headerLine.indexOf("=");
        String key = headerLine.substring(0, first);
        String value = headerLine.substring(first + 1);
        if (value.startsWith("<") && value.endsWith(">"))
        {
            this.fileHeaders.put(key, this.processIdType(value.substring(1, value.length() - 1)));
        }
        else
        {
            this.fileHeaders.put(key, value);
        }
    }

    private Map<String, Object> processIdType(String line)
    {
        Map<String, Object> result = new HashMap<String, Object>();
        String[] components = line.split(",");
        for (String component : components)
        {
            String[] kv = component.split("=");
            if (kv.length > 1)
            {
                result.put(kv[0], kv[1]);
            }
            else
            {
                result.put(kv[0], null);
            }
        }
        return result;
    }

    private static List<String> getFiles(String[] args)
    {
        File tempFile;
        List<String> result = new LinkedList<String>();
        for (String arg : args)
        {
            tempFile = new File(arg);
            if (tempFile.isFile() && arg.endsWith(".vcf"))
            {
                result.add(arg);
            }
            else if (tempFile.isDirectory())
            {
                // Add all the VCF files in the directory
                for (File file : tempFile.listFiles())
                {
                    if (file.isFile() && file.getName().endsWith(".vcf"))
                    {
                        result.add(file.getAbsolutePath());
                    }
                }
            }
        }
        return result;
    }

    /**
     * Get a buffered reader from a file.
     * @param fileName
     * @return
     * @throws IOException
     */
    public BufferedReader fileToBufferedReader(String fileName)
            throws IOException
    {
        File file = new File(fileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        return bufferedReader;
    }

    private static boolean isPublic(String[] args)
    {
        boolean result = false;
        for (String arg : args)
        {
            if (arg.equals("-public"))
            {
                result = true;
            }
        }
        return result;
    }


    public static void main(String[] args)
    {
        try
        {
            // Load the property file
            Properties appProperties = new Properties();
            appProperties.load(new FileInputStream("loader.properties"));

            String url = appProperties.getProperty("mongodb.url");
            Integer port = new Integer(appProperties.getProperty("mongodb.port"));
            String dbName = appProperties.getProperty("mongodb.name");
            String user = appProperties.getProperty("mongodb.username");
            String password = appProperties.getProperty("mongodb.password");

            MongoClient mongoClient = new MongoClient(url, port);

            DB db = mongoClient.getDB(dbName);
            if(user != null && password !=null)
            {
                db.authenticate(user,password.toCharArray());
            }

            boolean isPublic = isPublic(args);
            List<String> files = getFiles(args);
            for (String file : files)
            {
                System.out.println("Processing File : " + file + " public? : " + isPublic);
                VCFProcessor processor;
                if (isPublic)
                {
                    processor = new VCFProcessor(file, db, PUBLIC_COLLECTION, PUBLIC_FILEMETA);
                }
                else
                {
                    processor = new VCFProcessor(file, db, STANDARD_COLLECTION, STANDARD_FILEMETA);

                }
                processor.process();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}