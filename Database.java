package fop.w10join;

// TODO Import the stuff you need

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {

    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

    public static Stream<Customer> processInputFileCustomer() {
        try {
            Stream<String> files = Files.readAllLines(Paths.get(baseDataDirectory.toString()+ "/customer.tbl")).stream();
            return files.map(mapToCustomer).filter(x -> x != null);
        } catch (IOException e) {
            System.out.println("Data path seems to be wrong");
        }
        return null;
    }

    private static Function<String, Customer> mapToCustomer = (line) -> {
        String[] c = line.split("[|]");
        char[] charArr = c[2].toCharArray();
        char[] charArr1 = c[4].toCharArray();
        char[] charArr2 = c[7].toCharArray();
        return new Customer(Integer.parseInt(c[1].substring(9)), charArr, Integer.parseInt(c[3]),
                charArr1, Float.parseFloat(c[5]), c[6], charArr2);
    };

    public static Stream<LineItem> processInputFileLineItem() {
        try {
            Stream<String> files = Files.readAllLines(Paths.get(baseDataDirectory.toString()+ "/lineitem.tbl")).stream();
            return files.map(mapToLineItem).filter(x -> x != null);
        } catch (IOException e) {
            System.out.println("Data path seems to be wrong");
        }
        return null;
        // For quantity of LineItems please use Integer.parseInt(str) * 100.
    }
    private static Function<String, LineItem> mapToLineItem = (line) -> {
        String[] l = line.split("[|]");
        char[] charArr = l[13].toCharArray();
        char[] charArr2 = l[14].toCharArray();
        char[] charArr3 = l[15].toCharArray();
        LocalDate localDate = LocalDate.parse(l[10]);
        LocalDate localDate1 = LocalDate.parse(l[11]);
        LocalDate localDate2 = LocalDate.parse(l[12]);

        return new LineItem(Integer.parseInt(l[0]), Integer.parseInt(l[1]),
                Integer.parseInt(l[2]), Integer.parseInt(l[3]),
                Integer.parseInt(l[4]) * 100, Float.parseFloat(l[5]), Float.parseFloat(l[6]),
                Float.parseFloat(l[7]), l[8].charAt(0), l[9].charAt(0), localDate, localDate1, localDate2, charArr, charArr2, charArr3);
    };

    public static Stream<Order> processInputFileOrders() {
        try {
            Stream<String> files = null;
            files = Files.readAllLines(Paths.get(baseDataDirectory.toString()+ "/orders.tbl")).stream();
            return files.map(mapToOrer).filter(x -> x != null);
        } catch (IOException e) {
            System.out.println("Data path seems top be wrong");
        }
        return null;
    }
    private static Function<String, Order> mapToOrer = (line) -> {
        String[] o = line.split("[|]");
        LocalDate localDate = LocalDate.parse(o[4]);
        char[] charArr = o[5].toCharArray();
        char[] charArr1 = o[6].toCharArray();
        char[] charArr2 = o[8].toCharArray();

        return new Order(Integer.parseInt(o[0]), Integer.parseInt(o[1]), o[2].charAt(0),
                Float.parseFloat(o[3]), localDate, charArr, charArr1, Integer.parseInt(o[7]), charArr2);
    };

    public Database() {
        //TODO
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) throws NullPointerException{
        List<Customer> custsList = processInputFileCustomer().toList();
        List<Customer> custList = custsList.stream().filter(c -> c.getMktsegment().equals(marketsegment)).toList();
        Map<String, List<Integer>> custKeyMap = new HashMap<>();
        custKeyMap.put(marketsegment, custList.stream().map(c -> c.getCustKey()).toList());
//        List<Integer> custKeyList = custList.stream().map(c -> c.getCustKey()).toList();

        List<Order> ordersList = processInputFileOrders().toList();
        List<Order> filteredOrders = new ArrayList<>();
        custKeyMap.get(marketsegment).stream().forEach(x -> {
            filteredOrders.addAll(ordersList.stream().filter(o -> o.getCustKey() == x).collect(Collectors.toList()));
        });

    //    List<Integer> orderKeyList = filteredOrders.stream().map(o -> o.getOrderKey()).toList();
        Map<String,List<Integer>> orderKeyMap = new HashMap<>();
        orderKeyMap.put(marketsegment, filteredOrders.stream().map(o -> o.getOrderKey()).toList());
        List<LineItem> filteredLineItems = new ArrayList<>();
        List<LineItem> lineItemsList = processInputFileLineItem().collect(Collectors.toList());
        orderKeyMap.get(marketsegment).stream().forEach(x -> {
            filteredLineItems.addAll(lineItemsList.stream().filter(l -> l.getOrderKey() == x).collect(Collectors.toList()));
        });
        Map<String, List<Integer>> lineItemsQuantity = new HashMap<>();
        lineItemsQuantity.put(marketsegment, filteredLineItems.stream().map(y -> y.getQuantity()).toList());
        // List<Integer> lineItemsQuantity = filteredLineItems.stream().map(y -> y.getQuantity()).toList();
        long sum = lineItemsQuantity.get(marketsegment).stream().mapToInt(i -> i).sum();
        //long sum = lineItemsQuantity.get(marketsegment).stream().reduce(0, (accumulator, element) -> accumulator + element);

        return sum/ filteredLineItems.size();
    }


    public static void main(String[] args) {
        System.out.println("AUTOMOBILE " + new Database().getAverageQuantityPerMarketSegment("AUTOMOBILE"));
        System.out.println("BUILDING " + new Database().getAverageQuantityPerMarketSegment("BUILDING"));
        System.out.println("MACHINERY " + new Database().getAverageQuantityPerMarketSegment("MACHINERY"));
        System.out.println("HOUSEHOLD " + new Database().getAverageQuantityPerMarketSegment("HOUSEHOLD"));
    }
}