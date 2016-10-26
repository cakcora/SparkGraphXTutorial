import java.io.*;

/**
 * Created by cxa123230 on 10/25/2016.
 */
public class J1 {

   public String read(String fileName) throws Exception{

       BufferedReader br = new BufferedReader(new FileReader(fileName));
       String line ="";
       StringBuffer bf = new StringBuffer();
       while((line=br.readLine())!=null){
          bf.append(line+"\n");
       }

       br.close();
       return bf.toString();

   }
    public void write(String fileName,String content) throws Exception{

        File file = new File(fileName);
        FileWriter out = new FileWriter(file);
        System.out.println(file.getAbsolutePath());
        BufferedWriter wr = new BufferedWriter(out);
        wr.write(content);
        wr.close();
    }

   public J1( ){

   }
}
