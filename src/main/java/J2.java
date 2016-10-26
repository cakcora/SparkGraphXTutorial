/**
 * Created by cxa123230 on 10/25/2016.
 */
public class J2 {

    public static void main(String []args) throws Exception
    {
        J1 j = new J1();
//        j.write("fly.txt","whereisitnow");
        String g = j.read("C:/Projects/DisagioData/dblpgraph.txt.txt");

        System.out.println(g);
    }}
