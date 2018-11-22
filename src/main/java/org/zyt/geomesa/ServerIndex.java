package org.zyt.geomesa;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.zyt.geomesa.GeomesaTools.GeomesaData;
import org.zyt.geomesa.GeomesaTools.GeomesaInsert;
import org.zyt.geomesa.GeomesaTools.GeomesaSearch;


import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Servlet implementation class ServerIndex
 */
public class ServerIndex extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String[] args = new String[]{"--hbase.catalog", "a", "--hbase.zookeepers", "192.168.1.133"};



    /**
     * @see HttpServlet#HttpServlet()
     */
    public ServerIndex() {
        super();
        // TODO Auto-generated constructor stub
    }

    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        PrintWriter out = response.getWriter();
        response.setHeader("Access-Control-Allow-Origin", "*");
        /* 星号表示所有的异域请求都可以接受， */
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");

        //得到请求的参数Map，注意map的value是String数组类型
        Map map = request.getParameterMap();
        Set<String> keySet = map.keySet();
//        for (String key : keySet) {
//           String[] values = (String[]) map.get(key);
//           for (String value : values) {
//               System.out.println(key+"="+value);
//           }
//        }
        keySet.contains("insert");
        if (keySet.contains("insert")){
            try {
                String[] args = new String[]{"--hbase.catalog", "a", "--hbase.zookeepers", "192.168.1.133", "--cleanup"};
                new GeomesaInsert(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                        .insertRoute();
                out.write("{\"status\":\"success\"}");
            }catch (Exception e){
                out.write("{\"status\":\"failed\"}");
            }
            return;
        }else {
            try {
                out.write(new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                        .getCarNum());
            }catch (Exception e) {
                e.printStackTrace(out);
            }
            return;
        }

//        ByteArrayOutputStream baos =  TaxiSearch.searchRoute("1391110839","1391110838", null, null, null);
//		ServletOutputStream out=response.getOutputStream();
//		out.write(baos.toByteArray());

//		response.setContentType("application/octet-stream;charset=UTF-8");
//		ByteArrayOutputStream baos=new ByteArrayOutputStream();
//        int intBits = Float.floatToIntBits(-74f);
//        int intBits2 = Float.floatToIntBits(40f);
//		baos.write(getBytes(intBits));
//		baos.write(getBytes(intBits2));
        /* 设置响应头允许ajax跨域访问 */

    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
//		String string = request.getParameter("params");
//		Type t  = new TypeToken<List<Map<String,String>>>(){}.getType();
//		List<Map<String,String>> list = new Gson().fromJson(request.getParameter("params"), t);
//		for(Map<String,String> map : list){
//		    System.out.print("pk:"+map.get("st"));
//		    System.out.println("\tname:"+map.get("et"));
//		}

        try {
            /* 设置响应头允许ajax跨域访问 */
            response.setHeader("Access-Control-Allow-Origin", "*");
            /* 星号表示所有的异域请求都可以接受， */
            response.setHeader("Access-Control-Allow-Methods", "GET,POST");


            // Search

            Type t = new TypeToken<Map<String, String>>() {
            }.getType();
            Map<String, String> map = new Gson().fromJson(request.getParameter("params"), t);


            String maxView = map.get("maxView");
            String st = map.get("st");
            String et = map.get("et");
            String carID = map.get("carID");
            String area1 = map.get("area1");
            String area2 = map.get("area2");
            String area3 = map.get("area3");
            String startArea = "";
            String endArea = "";
            if (area1 != null && area1 != "") {
                startArea = area1;
                endArea = area1;
            }
            if (area2 != null && area2 != "") {
                startArea = area2;
            }
            if (area3 != null && area3 != "") {
                endArea = area3;
            }

            ByteArrayOutputStream baos = new GeomesaSearch(args, new HBaseDataStoreFactory().getParametersInfo(), new GeomesaData())
                    .searchRoute(st, et, carID, startArea, endArea, maxView);
            ServletOutputStream out = response.getOutputStream();
            out.write(baos.toByteArray());
        } catch (Exception e) {

            PrintWriter pw = response.getWriter();
            e.printStackTrace(pw);
        }
    }
}
