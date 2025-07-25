package files;
import java.nio.file.*;
import java.nio.*;

public class CreateDirectory {
	 public static Path create(String path) {
		 try {
			 Path p = Paths.get(path);
			 
			 if(Files.exists(p)) {
				 System.out.println("El directorio ya existe");
			 }else {
				 Path donePath = Files.createDirectories(p);
				 System.out.println("Directory Created at "+donePath.toString());
				 return donePath;
			 }
		 } catch(Exception e) {
			 e.printStackTrace();
		 }
		 
		 return null;
	 }
}
