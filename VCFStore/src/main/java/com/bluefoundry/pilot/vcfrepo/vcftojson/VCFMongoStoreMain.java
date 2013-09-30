package com.bluefoundry.pilot.vcfrepo.vcftojson;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import java.io.File;
import java.io.FileInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * User: Jason T
 * Date: 9/29/13
 * Time: 9:03 PM
 *
 * This class is the Main class for the VCF Loader Example.
 */
public class VCFMongoStoreMain
{
    // Public Collection Names
    private static final String PUBLIC_COLLECTION = "publicvariants";
    private static final String PUBLIC_FILEMETA = "publicvcf";

    // Standard Collection Names
    private static final String STANDARD_COLLECTION = "variants";
    private static final String STANDARD_FILEMETA = "vcf";
    
    public VCFMongoStoreMain()
    {
        
    }

    /**
     * Runs the loader program.
     *
     * @param args
     * @throws Exception
     */
    private void run(String[] args)
            throws Exception
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
            processor.processVCF();
        }
        
    }

    /**
     * Get all the files from the files or directory passed in.
     *
     * @param args
     * @return
     */
    private List<String> getFiles(String[] args)
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
     * Determine if the public flag is set.
     * @param args
     * @return
     */
    private boolean isPublic(String[] args)
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
            VCFMongoStoreMain main = new VCFMongoStoreMain();
            main.run(args);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
