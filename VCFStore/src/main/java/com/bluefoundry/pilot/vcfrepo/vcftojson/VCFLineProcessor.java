package com.bluefoundry.pilot.vcfrepo.vcftojson;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * User: Jason T
 * Date: 9/18/13
 * Time: 9:53 PM
 *
 */
public class VCFLineProcessor
{
    String[] vcfHeaders;
    String fileID;

    public VCFLineProcessor(String[] vcfHeaders, String fileID)
    {
        this.vcfHeaders = vcfHeaders;
        this.fileID = fileID;
    }

    public Map<String, Object> processLine(String line)
    {
        StringTokenizer tokenizer = new StringTokenizer(line);
        int headerIndex = 0;
        Map<String, Object> structure = new HashMap<String, Object>();
        structure.put("__vcfid", fileID);
        while (tokenizer.hasMoreTokens())
        {
            String token = tokenizer.nextToken();
            String header = this.vcfHeaders[headerIndex];
            if (header.equals("INFO"))
            {
                structure.put(header, this.processInfoLine(token));
            } else if (header.equals("POS"))
            {
                try
                {
                    Integer pos = Integer.parseInt(token);
                    structure.put(header, pos);
                } catch (NumberFormatException e)
                {
                    System.out.print("Cannot convert position");
                    // TODO Is this appropriate?
                    structure.put("STRINGPOS", token);
                }
            } else if (header.equals("ALT"))
            {
                String[] alts = token.split(",");
                structure.put(header, alts);
            } else
            {
                structure.put(header, token);
            }

            headerIndex++;
        }
        //TODO:  Can we some how normalize the chromosome position?
        return structure;
    }

    public Map<String, Object> processInfoLine(String infoLine)
    {
        //TODO see if we are processing = appropriatly;
        String[] infos = infoLine.split(";");
        Map<String, Object> result = new HashMap<String, Object>();
        for (String info : infos)
        {
            String[] kv = info.split("=");
            if (kv.length > 1)
            {
                result.put(kv[0], kv[1]);
            } else
            {
                result.put(kv[0], null);
            }
        }
        return result;
    }
}
