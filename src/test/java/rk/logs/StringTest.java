package rk.logs;

/**
 * @Author rk
 * @Date 2018/11/23 21:09
 * @Description:
 **/
public class StringTest {
    public static void main(String[] args) {
        String path = "/standard/data-clean/access/2018/11/22/part";
        int index = path.indexOf("access/");
        System.out.println(path.substring(index+7, index+7+10).replaceAll("\\/","-"));

        String str = "[2018-11-23, 2018-11-22]";
        String dataSet = str.replaceAll("\\[|\\]", "");
        System.out.println(dataSet);





    }

}
