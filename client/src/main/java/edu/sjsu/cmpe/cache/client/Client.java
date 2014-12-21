package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        String[] nodes = {"http://localhost:3000","http://localhost:3001","http://localhost:3002"};
        CacheServiceInterface cache = new DistributedCacheService(nodes);

        System.out.println("putting (1 => x)");
        cache.insertAsynchValues(1, "x");
        System.out.println("Need 30 seconds Sleep...!!!");
        Thread.sleep(30000);
		String value = cache.getAsynchValues(1);
        System.out.println("Get Initial Value..."+value);
		
        System.out.println("Updating value of 1 -> putting (1 => y)");
        cache.insertAsynchValues(1, "y");
        System.out.println("Need 30 seconds Sleep...!!!");
        Thread.sleep(30000);
        value = cache.getAsynchValues(1);
        System.out.println("Get Updated Value..."+value);
		
        System.out.println("Exiting Cache Client...");
    }

}
