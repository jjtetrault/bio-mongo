package com.bluefoundry.pilot.vcfrepo.vcftojson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import java.io.*;
import java.util.*;

/**
 * User: Jason T
 * Date: 9/18/13
 * Time: 10:41 PM
 *
 * This class will process a VCF file and load the results into the 2 collections
 * provided in the constructor.
 */
public class VCFProcessor
{
    private String inFile;

    private Map<String, Object> fileHeaders;
    private DB mongoDB;
    private String variantCollection;
    private String vcfCollection;

    private final static int flushsize = 10000;

    UUID id = UUID.randomUUID();

    /**
     * Main Con
     * @param vcfFile Location of the VCF File
     * @param db MongoDB reference
     * @param variantCollection The collection name to put the actual variants in
     * @param vcfCollection  The file information.
     */
    public VCFProcessor(String vcfFile, DB db, String variantCollection, String vcfCollection)
    {
        this.inFile = vcfFile;
        fileHeaders = new HashMap<String, Object>();
        this.mongoDB = db;
        this.vcfCollection = vcfCollection;
        this.variantCollection = variantCollection;
    }

    /**
     * Process the VCF file
     *
     * @throws Exception
     */
    public void processVCF() throws Exception
    {
        String[] currentHeaders = null;
        BufferedReader bufferedReader = this.fileToBufferedReader(inFile);
        String line = bufferedReader.readLine();
        VCFLineProcessor processor = null;
        List<DBObject> variantBuffer = new LinkedList<DBObject>();

        while (line != null)
        {
            if (line.startsWith("##"))
            {
                processHeaders(line);
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

            line = bufferedReader.readLine();

        }
        // do a final flush of the buffer.
        this.flushBuffer(variantBuffer);

        Gson doc = new GsonBuilder().create();
        this.fileHeaders.put("__vcfid", id.toString());
        String fileJSON = doc.toJson(this.fileHeaders);

        DBObject dbObject = (DBObject) JSON.parse(fileJSON);
        DBCollection collection = mongoDB.getCollection(this.vcfCollection);
        collection.insert(dbObject);
    }

    /**
     * Flushes the buffer of variants.  THis is used to increase loading performance.
     *
     * @param variants
     */
    private void flushBuffer(List<DBObject> variants)
    {
        DBCollection collection = mongoDB.getCollection(variantCollection);
        collection.insert(variants);
        System.out.println("Flushed Buffer");
    }

    /**
     * Processes the headers for the file.
     *
     * @param headerLine
     */
    private void processHeaders(String headerLine)
    {
        // Get rid of the ##
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

    /**
     * Processes the ID
     *
     * @param line
     * @return
     */
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

    /**
     * Get a buffered reader from a file.
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    private BufferedReader fileToBufferedReader(String fileName)
            throws IOException
    {
        File file = new File(fileName);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        return bufferedReader;
    }
}