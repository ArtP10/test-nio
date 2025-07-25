package files;

import java.nio.file.*;

public class CreateFile {
	public static void create(String path) {
		try {
			Path p = Paths.get(path);
			if(Files.exists(p)) {
				System.out.println("Ya existe el archvo");
				
			}else {
				Path donePath = Files.createFile(p);
				System.out.println("Archivo creado en: " +donePath.toString());
			}
		} catch(Exception e) {
			
		}
	}

}
