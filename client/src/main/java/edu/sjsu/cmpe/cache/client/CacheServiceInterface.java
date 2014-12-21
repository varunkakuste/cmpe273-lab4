package edu.sjsu.cmpe.cache.client;

/**
 * Cache Service Interface
 * 
 */
public interface CacheServiceInterface {
    public String get(long key);
    public void put(long key, String value);
	public String getAsynchValues(long key);
    public void insertAsynchValues(long key, String value);
}
