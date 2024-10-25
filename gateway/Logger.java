package it.polimi.gateway;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    
    public static void Log(String message) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        String formattedTime = now.format(formatter);

        System.out.println("[" + formattedTime + "]" + message);
    }
}
