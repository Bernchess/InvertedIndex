import java.io.*;

public class InputSmall {
    public static void main(String[] args) throws IOException {
        File file1 = new File(args[0]);
        File[] files = file1.listFiles();

        for(File f : files) {
            FileInputStream fileInputStream = new FileInputStream(f.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
            String line = null;
            int limitRaws = 500;
            int raw = 0;
            String newFileName = f.getName();
            if("古".equals(String.valueOf(f.getName().charAt(0))) || "金".equals(String.valueOf(f.getName().charAt(0))) || "李".equals(String.valueOf(f.getName().charAt(0)))) {
                newFileName = newFileName.substring(0, 2)+".txt"+ newFileName.substring(2);
            } else {
                newFileName = newFileName.substring(0, 3)+".txt"+ newFileName.substring(3);
            }
            File file = new File(args[1] + newFileName);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            while ((line = bufferedReader.readLine()) != null && raw < limitRaws) {
                raw++;
                fileOutputStream.write(line.getBytes());
                fileOutputStream.write("\n".getBytes());
            }
            fileOutputStream.close();
            fileInputStream.close();
        }
    }
}
