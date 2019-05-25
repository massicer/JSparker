package utility;

public class LogManager {

    private static LogManager shared;

    public static LogManager getShared(){
        if(shared == null) shared = new LogManager();
        return shared;
    }

    public void logInfo(String msg){
        // Don't forget to use the RESET after printing as the effect will remain if it's not cleared
        System.out.println(ConsoleColors.CYAN +"[INFO] "+msg+
                ConsoleColors.RESET );
    }

    public void logError(String msg){
        // Don't forget to use the RESET after printing as the effect will remain if it's not cleared
        System.out.println(ConsoleColors.RED +"[ERROR] "+msg+
                ConsoleColors.RESET );
    }

    public void logWarning(String msg){
        // Don't forget to use the RESET after printing as the effect will remain if it's not cleared
        System.out.println(ConsoleColors.YELLOW +"[WARNING] "+msg+
                ConsoleColors.RESET );
    }

    public void logSuccess(String msg){
        // Don't forget to use the RESET after printing as the effect will remain if it's not cleared
        System.out.println(ConsoleColors.GREEN +"[SUCCESS] "+msg+
                ConsoleColors.RESET );
    }
}
