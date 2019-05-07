package com.tq.udf.person;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;



import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.google.i18n.phonenumbers.geocoding.PhoneNumberOfflineGeocoder;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author tq
 * @date 2019/5/5 16:10
 */
public class GetTelDetail extends GenericUDTF {
    public static void main(String[] args) throws HiveException {


//        String[] result = getInfo("15967193899asd");
//        System.out.println(result[0]);
//        System.out.println(result[1]);
//
//        Object[] objects = {"qwe",null};
//        new GetTelDetail().process(objects);
    }

    public static String[] getInfo(String tel) {

        String regex = "[^0-9]";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(tel);
        if(m.replaceAll("").length()==11) {
            tel = m.replaceAll("");
        }else {
            tel = "123";
        }
       // System.out.println(tel);

        String url = "https://tcc.taobao.com/cc/json/mobile_tel_segment.htm?tel=".concat(tel);

        StringBuilder json = new StringBuilder();
        try {
            URL oracel = new URL(url);
            // System.out.println(oracel);
            URLConnection yc = oracel.openConnection();
            // System.out.println(yc);
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    yc.getInputStream(), "GBK"
            ));
            String inputLine = null;
            while ((inputLine = in.readLine()) != null) {
                json.append(inputLine);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // System.out.println(json);
        String b = json.toString().replace("__GetZoneResult_ = ", "").replace("'", "\"").replace(",", ",\"").replace("{", "{\"").replace(":", "\":").replace(" ", "");

        // System.out.println(b.contains("catName"));
        String[] result = new String[2];
        PhoneNumberUtil phoneNumberUtil = PhoneNumberUtil.getInstance();
        PhoneNumberOfflineGeocoder phoneNumberOfflineGeocoder = PhoneNumberOfflineGeocoder.getInstance();

        String language = "CN";
        Phonenumber.PhoneNumber referencePhonenumber = null;


        try {
            referencePhonenumber = phoneNumberUtil.parse(tel, language);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String city = phoneNumberOfflineGeocoder.getDescriptionForNumber(referencePhonenumber, Locale.CHINA);

        result[1] = city;
        if (b.contains("catName")) {
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(b).getAsJsonObject();

            // System.out.println(jsonObject.has("catName"));
            result[0] = jsonObject.get("catName").getAsString();
            return result;
        } else {
            result[0] = "未知";
            result[1] = "未知";
            return result;
        }
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentLengthException("ExplodeMap takes only two argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE || args[1].getCategory() != ObjectInspector.Category.PRIMITIVE ) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col3");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

        String tel;
        if (objects[1] == null || objects[1] == "") {
            tel = "123";
        } else {
            tel = objects[1].toString();
        }


        String[] arr = getInfo(tel);


        String[] result = {objects[0].toString(), arr[0], arr[1]};
//        System.out.println(result[0]);
//        System.out.println(result[1]);
//        System.out.println(result[2]);
        forward(result);
    }

    @Override
    public void close() throws HiveException {

    }
}
