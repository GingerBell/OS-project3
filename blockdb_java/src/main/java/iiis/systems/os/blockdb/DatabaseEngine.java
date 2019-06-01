package iiis.systems.os.blockdb;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.channels.FileChannel;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    // helper function to convert a line in the log file to Transaction.Builder
    private boolean logLineToTransactionBuilder(String line, Transaction.Builder transaction) {
        String[] splitLine = line.split(" ");
        if (splitLine.length <= 0) {
            System.out.println("Unexpected empty line in " + dataDir + "log.txt");
            return false;
        }
        switch (splitLine[0]) {
           case "PUT":
                if (splitLine.length != 3) {
                    System.out.println("PUT transaction records should be in format: PUT userID value.");
                    return false;
                }
                transaction.setType(Transaction.Types.PUT).setUserID(splitLine[1]).setValue(Integer.parseInt(splitLine[2]));
                break;
            case "DEPOSIT":
                if (splitLine.length != 3) {
                    System.out.println("DEPOSIT transaction records should be in format: DEPOSIT userID value.");
                    return false;
                }
                transaction.setType(Transaction.Types.DEPOSIT).setUserID(splitLine[1]).setValue(Integer.parseInt(splitLine[2]));
                break;
            case "WITHDRAW":
                if (splitLine.length != 3) {
                    System.out.println("WITHDRAW transaction records should be in format: WITHDRAW userID value.");
                    return false;
                }
                transaction.setType(Transaction.Types.WITHDRAW).setUserID(splitLine[1]).setValue(Integer.parseInt(splitLine[2]));
                break;
            case "TRANSFER":
                if (splitLine.length != 4) {
                    System.out.println("TRANSFER transaction records should be in format: TRANSFER fromID toID value.");
                    return false;
                }
                transaction.setType(Transaction.Types.TRANSFER).setFromID(splitLine[1]).setToID(splitLine[2]).setValue(Integer.parseInt(splitLine[3]));
                break;
            default:
                System.out.println("A log record should start with PUT, DEPOSIT, WITHDRAW or TRANSFER.");
                return false;
        }
        return true;
    }

    // helper function to update balances by a Transaction
    private boolean updateWithTransaction(Transaction transaction) {
         if (transaction.getValue() < 0)
            return false;
        switch(transaction.getType().getNumber()) {
            case 2:
                balances.put(transaction.getUserID(), transaction.getValue());
                break;
            case 3:
                balances.put(transaction.getUserID(), getOrZero(transaction.getUserID()) + transaction.getValue());
                break;
            case 4:
                int balance = getOrZero(transaction.getUserID()) - transaction.getValue();
                if (balance < 0)
                    return false;
                balances.put(transaction.getUserID(), balance);
                break;
            case 5:
                int fromBalance = getOrZero(transaction.getFromID()) - transaction.getValue();
                int toBalance = getOrZero(transaction.getToID()) + transaction.getValue();
                if (fromBalance < 0)
                    return false;
                balances.put(transaction.getFromID(), fromBalance);
                balances.put(transaction.getToID(), toBalance);
                break;
            default:
                return false;
        }
        return true;
    }

    // initialize the database with json files and log.txt
    private void initialize() {
        File file = new File(dataDir + "log.txt");
        // clean start
        if (!file.exists()) {
            try (FileWriter writer = new FileWriter(file)) {
                writer.write("1\n");
            } catch (IOException e) {
                System.out.println("Cannot write to file " + dataDir + "log.txt.");
            }
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            blockId = Integer.parseInt(line);
        } catch (IOException e) {
            System.out.println("Cannot read file " + dataDir + "log.txt.");
            return;
        }

        // if the previous process was killed before copying log_ to log in flushToBlock
        File file_ = new File(dataDir + "log_.txt");
        if (file_.exists()) {
            System.out.println("The previous process was killed before copying log_ to log in flushToBlock. Recopying.");
            blockId ++;
            try (FileWriter writer = new FileWriter(dataDir + "log_.txt")) {
                writer.write(blockId + "\n");
            } catch (IOException e) {
                System.out.println("Cannot write to file " + dataDir + "log_.txt");
                return;
            }
            try {
                Files.move(Paths.get(dataDir + "log_.txt"), Paths.get(dataDir + "log.txt"), StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                System.out.println("Cannot replace log.txt with log_.txt.");
                return;
            }
        }

        // if the previous process was killed before finishing writing to blockId.json
        File fileJson = new File(dataDir + blockId + ".json");
        if (fileJson.exists()) {
            System.out.println("The previous process was killed before finishing writing to blockId.json. Reflushing to block.");
            if (!flushToBlock()) {
                System.out.println("Cannot flush to block. Database initialization failed.");
                return;
            }
        }

        // update the database using transaction records in the block files
        for (int i = 1; i < blockId; i ++) {
            String json;
            try {
                json = new String(Files.readAllBytes(Paths.get(dataDir + i + ".json"))); 
            } catch (IOException e) {
                System.out.println("Cannot read file " + dataDir + i + ".json. Database initialization failed.");
                return;
            }
            Block.Builder block = Block.newBuilder();
            try {
                JsonFormat.parser().merge(json, block);
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Failed to convert JSON to block. Database initialization failed.");
                return;
            }
            if (block.getTransactionsCount() != N) {
                System.out.println("There should be " + N + " transactions in " + dataDir + i + ".json, but get " + block.getTransactionsCount() + " transactions. Database initialization failed.");
                return;
            }
            for (int j = 0; j < N; j ++) {
                Transaction transaction = block.getTransactions(j);
                if (!updateWithTransaction(transaction)) {
                    System.out.println("The " + j + "th record is inconsistent in " + dataDir + i + ".json. Database initialization failed.");
                    return;
                }
            }
        }

        // update the database using log file
        long startPoint = 0;
        String line;
        try (BufferedReader reader = new BufferedReader(new FileReader(dataDir + "log.txt"))) {
            line = reader.readLine();
            startPoint += line.getBytes().length + 1;
            line = reader.readLine();
            String nextLine;
            while ((nextLine = reader.readLine()) != null && line != null) {
                startPoint += line.getBytes().length + 1;
                Transaction.Builder transaction = Transaction.newBuilder();
                if (!logLineToTransactionBuilder(line, transaction)) {
                    System.out.println("Incomplete record in log file. Database initialization failed.");
                    return;
                }
                if (!updateWithTransaction(transaction.build())) {
                    System.out.println("Inconsistent record in log file. Database initialization failed.");
                    return;
                }
                logLength ++;
                line = nextLine;
            }

        } catch (IOException e) {
            System.out.println("Cannot read file " + dataDir + "log.txt");
            return;
        }

        // the last line of log.txt may be incomplete, need to delete it if so
        if (line != null) {
            Transaction.Builder transaction = Transaction.newBuilder();
            if (!logLineToTransactionBuilder(line, transaction)) {
                System.out.println("The last record is incomplete in log file. Need to be deleted.");
                try (FileChannel outChan = new FileOutputStream(dataDir + "log.txt", true).getChannel()) {
                    outChan.truncate(startPoint);
                } catch (IOException e) {
                    System.out.println("Cannot truncate file log.txt.");
                }
                return;
            }
            if (!updateWithTransaction(transaction.build())) {
                System.out.println("Inconsistent record in log file. Database initialization failed.");
            }
            logLength ++;
        }
    }

    public static void setup(String dataDir, int N) {
        instance = new DatabaseEngine(dataDir, N);
    }

    private HashMap<String, Integer> balances = new HashMap<>();
    private int logLength = 0;
    private String dataDir;
    private int N = 50;
    private int blockId = 1;

    DatabaseEngine(String dataDir, int N) {
        this.dataDir = dataDir;
        this.N = N;
        this.initialize();
    }

    // flush the N transactions in log.txt to a json file
    private boolean flushToBlock() {
        Block.Builder block = Block.newBuilder().setBlockID(blockId).setPrevHash("00000000").setNonce("00000000");
        try (BufferedReader reader = new BufferedReader(new FileReader(dataDir + "log.txt"))) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                Transaction.Builder transaction = Transaction.newBuilder();
                if (!logLineToTransactionBuilder(line, transaction)) 
                    return false;
                block.addTransactions(transaction.build());
            }
        } catch (IOException e) {
            System.out.println("Cannot read file " + dataDir + "log.txt");
            return false;
        }

        String jsonString;
        try {
            jsonString = JsonFormat.printer().print(block.build());
        } catch (InvalidProtocolBufferException e) {
            System.out.println("Failed to convert block to JSON.");
            return false;
        }

        try (FileWriter writer = new FileWriter(dataDir + blockId + ".json")) {
            writer.write(jsonString);
        } catch (IOException e) {
            System.out.println("Cannot write to file " + dataDir + blockId + ".json");
            return false;
        }

        blockId ++;
        try (FileWriter writer = new FileWriter(dataDir + "log_.txt")) {
            writer.write(blockId + "\n");
        } catch (IOException e) {
            System.out.println("Cannot write to file " + dataDir + "log_.txt");
            return false;
        }
        try {
            Files.move(Paths.get(dataDir + "log_.txt"), Paths.get(dataDir + "log.txt"), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.out.println("Cannot replace log.txt with log_.txt.");
            return false;
        }
        logLength = 0;
        return true;
    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    public int get(String userId) {
        return getOrZero(userId);
    }

    public boolean put(String userId, int value) {
        if (value < 0)
            return false;

        try (FileWriter writer = new FileWriter(dataDir + "log.txt", true)) {
            writer.write("PUT " + userId + " " + value + "\n");
        } catch (IOException e) {
            System.out.println("Cannot write to file " + dataDir + "log.txt");
            return false;
        }
        logLength ++;
        if (logLength >= N)
            flushToBlock();
        balances.put(userId, value);
        return true;
    }

    public boolean deposit(String userId, int value) {
        if (value < 0)
            return false;

        try (FileWriter writer = new FileWriter(dataDir + "log.txt", true)) {
            writer.write("DEPOSIT " + userId + " " + value + "\n");
        } catch (IOException e) {
            System.out.println("Cannot write to file " + dataDir + "log.txt");
            return false;
        }
        logLength ++;
        if (logLength >= N)
            flushToBlock();
        int balance = getOrZero(userId);
        balances.put(userId, balance + value);
        return true;
    }

    public boolean withdraw(String userId, int value) {
        int balance = getOrZero(userId);
        if (value < 0 || balance - value < 0)
            return false;

        try (FileWriter writer = new FileWriter(dataDir + "log.txt", true)) {
            writer.write("WITHDRAW " + userId + " " + value + "\n");
        } catch (IOException e) {
            System.out.println("Cannot write to file " + dataDir + "log.txt");
            return false;
        }
 
        logLength ++;
        if (logLength >= N)
            flushToBlock();
        balances.put(userId, balance - value);
        return true;
    }

    public boolean transfer(String fromId, String toId, int value) {
        int fromBalance = getOrZero(fromId);
        int toBalance = getOrZero(toId);
        if (value < 0 || fromBalance - value < 0)
            return false;

        try (FileWriter writer = new FileWriter(dataDir + "log.txt", true)) {
            writer.write("TRANSFER " + fromId + " " + toId + " " + value + "\n");
        } catch (IOException e) {
            System.out.println("Cannot write to file " + dataDir + "log.txt");
            return false;
        }

        logLength ++;
        if (logLength >= N)
            flushToBlock();
        balances.put(fromId, fromBalance - value);
        balances.put(toId, toBalance + value);
        return true;
    }

    public int getLogLength() {
        return logLength;
    }

}
