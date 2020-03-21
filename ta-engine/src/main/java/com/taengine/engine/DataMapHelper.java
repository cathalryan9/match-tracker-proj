package com.taengine.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONObject;

public class DataMapHelper {
	


	public Map<String, Integer> wordCountMap = new TreeMap<String, Integer>();
	// map of hashtags
	public Map<String, Integer> hashtagCountMap = new TreeMap<String, Integer>();
	
	public Map<String, Integer> currentIntervalMap = new HashMap<String, Integer>();
	
	public Map<Long, String> intervalsMap = new TreeMap<Long, String>();
	
	Date currentTime = null;
	
	public void addToInterval(String s) {
		JSONObject JO = new JSONObject(s);
		if (this.currentTime == null) {
			this.currentTime = new Date(Long.parseLong(JO.get("timestamp").toString()));
		} else if (Long.parseLong(JO.get("timestamp").toString()) > this.currentTime.getTime()) {

			System.out.println("end of interval");
			// error on line 166 v
			String word = Collections.max(this.currentIntervalMap.entrySet(), Map.Entry.comparingByValue()).getKey();
			this.intervalsMap.put(Long.parseLong(JO.get("timestamp").toString()), word);
			this.currentIntervalMap.clear();
			this.increaseTime(60000);
		}
		
	}
	
	public void increaseTime(long interval) {
		currentTime.setTime(currentTime.getTime() + interval);
	}
	
	
	public static List<JSONObject> createSortedList(Map<String, Integer> m) {
		if (!m.isEmpty()) {
			List<JSONObject> messageList = new ArrayList<JSONObject>();
			for (Map.Entry<String, Integer> entry : m.entrySet()) {
				// construct json object
				JSONObject JO = new JSONObject();
				JO.put("text", entry.getKey());
				JO.put("count", entry.getValue());
				String json = JO.toString();
				messageList.add(JO);
			}
			messageList.sort((o1, o2) -> {
				if ((int) o1.get("count") < (int) o2.get("count")) {
					return 1;
				} else if ((int) o1.get("count") > (int) o2.get("count")) {
					return -1;
				}
				return 0;
			});
			// If the number of words is > 30, take the largest 30 by count
			if (messageList.size() > 30) {
				messageList = messageList.subList(0, 30);
			}
			return messageList;
		}
		return null;
	}

}
