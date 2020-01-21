import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import sun.misc.BASE64Decoder;

import java.io.*;
import java.net.URLEncoder;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * 该代码的整体过程为
 * 从数据库中的user表中读取数据，将数据存入生成的xml文件中，其中从数据库中读取的blob字段转换为base64编码存进xml
 * 将xml分别存入本地以及数据库中的xmlfile表中
 * 对于xml文件进行解析
 * 将xml文件中的数据解析后存进与user表结构相同的usernew表中
 */
public class TestForProject {
    static String preEle = "//YGCT/MSG";
    static Connection connection = null;
    static Statement statement = null;
    public static void main(String[] args) throws SQLException, IOException {
        try{
            ResultSet resultSet = null;
            String sql = "select * from user";
            connection = JDBCUtils.getConnection();
            statement = connection.createStatement();

            //查找数据库表中的数据
            resultSet = statement.executeQuery(sql);

            //生成xml文件
            Document document = DocumentHelper.createDocument();
            Element root = document.addElement("YGCT");//xml根节点
            root.addAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance").addAttribute("xmlns:xsd",
                    "http://www.w3.org/2001/XMLSchema");
            Element HEAD = root.addElement("HEAD");///头节点
            HEAD.addElement("VERSION").addText("1.0");
            HEAD.addElement("SRC").addText("dczzbm");
            HEAD.addElement("DES").addText("0000000000");////6.1代码,国电集团,10个0
            HEAD.addElement("MsgNo").addText("msgno"); // 添加HEAD里面的一些属性
            HEAD.addElement("MsgId").addText("ls_id");//长度可能超过dczzbm+msgid+DJBH
            HEAD.addElement("MsgRef");
            HEAD.addElement("TransDate").addText("ls_id");
            HEAD.addElement("Reserve");
            Element preElement = root.addElement("MSG");

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            System.out.println("共有"+columnCount+"列");

            String[] elementNames = new String[columnCount];
            Element[] elements = new Element[columnCount];
            String[] elementType = new String[columnCount];

            System.out.println("5列数据分别为：");
            for (int i = 0; i < columnCount; i++) {
                elementNames[i] = resultSetMetaData.getColumnName(i+1);
                System.out.println(elementNames[i]);
            }
            System.out.println("5个类型的分类为");
            for (int i = 0; i < columnCount; i++) {
                int type = resultSetMetaData.getColumnType(i + 1);
                System.out.println("当前的类型为："+type);
                //由于本次测试采用的是MySQL数据库，所以对于blob字段的读取，可能读取出来的类型为LONGVARBINARY
                if (type==Types.LONGVARBINARY || type==Types.BLOB){
                    //BLOB默认为2004
                    elementType[i]="0";
                }else if (Types.DATE == type) {
                    //DATE默认为91
                    elementType[i] = "1";
                } else {
                    //其它
                    elementType[i] = "2";
                }
            }


            //对由数据库中查询出来的数据进行遍历，将其放在xml文件中
            while (resultSet.next()){
                org.dom4j.Element data = preElement.addElement("MAINDATA");
                for (int i = 0; i < columnCount; i++) {
                    elements[i] =data.addElement(elementNames[i]);
                    System.out.println("elementType[i]:        "+elementType[i]);
                    if (elementType[i].equals("0")){//二进制大文本
                        System.out.println(elementNames[i]);
                        Blob blob = resultSet.getBlob(elementNames[i]);
                        StringBuffer stringBuffer;
                        String ls_base64 = cvtBlobToBs64String(blob);//blob 转换为BASE64编码
                        System.out.println("ls_base64   blob:   "+ls_base64);
                        //base64ToFile("D:\\没用的东西",ls_base64,"test.xml");
                        int len2 = ls_base64.length(); //长度
                        elements[i].setText(ls_base64.substring(0, len2));//

                    }
                    else if (elementType[i].equals("1")) {//日期类型
                        Date date1 = resultSet.getDate(elementNames[i]);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                        String str = sdf.format(date1);
                        elements[i].setText(str);
                    }else {//其它类型
                        if (null == resultSet.getString(elementNames[i])) {
                            elements[i].setText("");
                        } else {
                            //需转换为UTF8编码,20180601.?
                            String string = resultSet.getString(elementNames[i]);
                            System.out.println("ls_val:    "+string);
                            try{
                                //判断原本的格式是不是utf-8，如果是，则存进xml，如果不是，将其转换为utf-8
                                if (!string.equals(new String(string.getBytes(),"utf-8"))){
                                    System.out.println("当前字符串格式不是utf-8");
                                    //此处将字符串转换为utf-8
                                    StringBuffer stringBuffer = new StringBuffer();
                                    stringBuffer.append(string);
                                    String utf8String="" ;
                                    String xmString="" ;
                                    try{
                                        xmString  = new String(stringBuffer.toString().getBytes("utf-8"));
                                        utf8String = URLEncoder.encode(xmString,"utf-8");
                                        elements[i].setText(utf8String);
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                }else{
                                    elements[i].setText(string);
                                }
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                            System.out.println("---------------------------------------------");
                        }
                    }
                }
                System.out.println("data         "+data);
            }

            //将xml文件存放至本地
            XmltoFile("D:\\\\没用的东西\\testfinal.xml",document);

            //将xml文件存在数据库中的xmlfile表格中
            String insertSql = "insert into xmlfile values(?,'test')";
            ByteArrayInputStream in = new ByteArrayInputStream(document.asXML().getBytes());
            PreparedStatement ps = connection.prepareStatement(insertSql);
            ps.setBinaryStream(1,in,(int)(document.asXML().getBytes().length));
            ps.executeUpdate();

            //对xml文件进行读取
            String searchSql = "select * from xmlfile where desbm = 'test'";     //只针对测试用例，所以只查找一个xml文档
            ResultSet resultSet1 = statement.executeQuery(searchSql);
            resultSet1.next();
            Blob blob = resultSet1.getBlob(1);

            byte[] bytes = blob.getBytes(1, (int) blob.length());
            String str = new String(bytes,"utf-8");
            System.out.println(str);

            Document doc = DocumentHelper.parseText(str);
            //该方法是考虑了每一个节点还有子节点的情况，但是此处只为测试base64的解码，所以不去考虑子节点的情况
            docToDb(doc, "usernew", "0", "0", "0", "0", "0", "0");

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            JDBCUtils.close(statement,connection);
        }

    }




    public static void docToDb(Document doc, String maintable, String childtable1,
                               String eleName1, String childtable2, String eleName2, String childtable3, String eleName3) {
        List itemList = doc.selectNodes(preEle + "/MAINDATA");
        insertDataToTable(maintable, itemList);
        if (!childtable1.equals("0")) {
            List itemList1 = doc.selectNodes(preEle + "/MAINDATA/SUBDATA/" + eleName1);
            insertDataToTable(childtable1, itemList1);
        }
        if (!childtable2.equals("0")) {
            List itemList2 = doc.selectNodes(preEle + "/MAINDATA/SUBDATA/" + eleName2);
            insertDataToTable(childtable2, itemList2);
        }
        if (!childtable3.equals("0")) {
            List itemList3 = doc.selectNodes(preEle + "/MAINDATA/SUBDATA/" + eleName3);
            insertDataToTable(childtable3, itemList3);
        }
        try {
            if (itemList != null) {
                itemList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //将从xml文档中读取到的数据存到新的一张表中
    public static void insertDataToTable(String datatable, List itemList) {
        ResultSet result = null;
        String sql = "select * from " + datatable;
        int columnCount = 0;
        String str = "";
        StringBuffer sbString = new StringBuffer();
        sbString.append("insert into " + datatable + " values(");
        try {

            result = statement.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = result.getMetaData();  //获取数据类型
            //获取数据库列数
            columnCount = resultSetMetaData.getColumnCount();

            String[] elements = new String[columnCount];
            String[] elementNames = new String[columnCount];
            String[] elementsTypes = new String[columnCount];


            for (int i = 0; i < columnCount; i++) {
                int type = resultSetMetaData.getColumnType(i + 1);
                if (type == Types.VARCHAR) {
                    elementsTypes[i] = "0";
                } else if (type == Types.NUMERIC) {
                    elementsTypes[i] = "1";
                } else if (type == Types.DATE) {
                    elementsTypes[i] = "2";
                } else if (type == Types.BLOB  || type == Types.LONGVARBINARY) {
                    elementsTypes[i] = "3";
                }
            }

            for (int i = 0; i < columnCount; i++) {
                //有多少列，就有多少和问号添加到sql语句中
                elementNames[i] = resultSetMetaData.getColumnName(i + 1);
                if (i != columnCount - 1)
                    str += "?,";
                else
                    str += "?";
            }
            sbString.append(str + ")");

            String mainSql = sbString.toString();  //数据插入语句
            PreparedStatement ps = null;
            ps = connection.prepareStatement(mainSql);

            //对于List列表进行遍历，
            for (Iterator iter = itemList.iterator(); iter.hasNext(); ) {
                Element el = (Element) iter.next();
                for (int i = 0; i < columnCount; i++) {
                    elements[i] = el.elementText(elementNames[i]);
                }
                for (int i = 0; i < columnCount; i++) {                    //
                    if (elementsTypes[i] == "2") {
                        ps.setObject(i + 1, utilToSql(elements[i]));//日期
                    } else if (elementsTypes[i] == "3") {              //大文本数据
                        //需添加 base64解码.原本在xml 中为base64编码，现在需要变为Blob
                        System.out.println(elements[i]);//这里是一个base64 的字符串，下面需要将它转换为blob从而存进数据库
                        //现将base64转换为byte数组
                        byte[] bytes = transformBase64(elements[i]);
                        Blob blob = fromByteArrayToBlob(bytes);
                        ps.setBlob(i+1,blob);
                    } else {
                        ps.setObject(i + 1, elements[i]);
                    }
                }
                ps.executeUpdate();

            }
            if (ps != null) {
                ps.close();
            }
            if (result != null) {
                result.close();
            }
            sbString = null;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //对于时间格式进行格式排版
    public static java.sql.Date utilToSql(String date) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            java.util.Date date1 = sdf.parse(date);
            return new java.sql.Date(date1.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


    //将base64编码转换为byte数组
    public static byte[] transformBase64(String str) {
        BASE64Decoder decode = new BASE64Decoder();
        byte[] b = null;
        try {
            b = decode.decodeBuffer(str);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b;
    }

    //将base64编码转换为文件
    public static void base64ToFile(String destPath, String base64, String fileName) {
        File file = null;
        //创建文件目录
        String filePath = destPath;
        File dir = new File(filePath);
        if (!dir.exists() && !dir.isDirectory()) {
            dir.mkdirs();
        }
        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        try {
            byte[] bytes = Base64.getDecoder().decode(base64);
            file = new File(filePath + "/" + fileName);
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            bos.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //Blob转BASE64
    public static String cvtBlobToBs64String(Blob in_blob) {
        StringBuffer result = null;
        String ls_result = "";
        Base64.Encoder encoderBase64 = Base64.getEncoder();
        if (null != in_blob) {
            try {
                InputStream msgCnt = in_blob.getBinaryStream();
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                byte[] buffer1 = new byte[2000];//每次读取2000个字节
                int n = 0;
                while (-1 != (n = msgCnt.read(buffer1))) {
                    output.write(buffer1, 0, n);
                }
                //result.append(encoderBase64.encode(output.toByteArray()));
                //result.append(encoderBase64.encodeToString(output.toByteArray()));
                ls_result = encoderBase64.encodeToString(output.toByteArray());//编码
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return ls_result;
            //return result;
        } else {
            return null;
        }
    }

    //将byte数组转换为blob对象
    public static Blob fromByteArrayToBlob(byte[] bytes){
        Configuration configure = new Configuration().configure();
        SessionFactory sessionFactory = configure.buildSessionFactory();
        Session session = sessionFactory.openSession();
        Blob blob = session.getLobHelper().createBlob(bytes);
        String string = cvtBlobToBs64String(blob);
        System.out.println(string);
        return blob;
    }

    //将xml放入本地测试2
    public static void XmltoFile(String filename,Document document) throws IOException {
        OutputFormat outputFormat = OutputFormat.createPrettyPrint();
        outputFormat.setLineSeparator("\r\n");//这是为了换行操作
        Writer writer = new FileWriter(filename);
        XMLWriter outPut = new XMLWriter(writer,outputFormat);
        outPut.write(document);
        outPut.close();
    }
}
