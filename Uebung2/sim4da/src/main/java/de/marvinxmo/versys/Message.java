package de.marvinxmo.versys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Message {

    private Map<String, Map<String, String>> content = new HashMap<>();
    private static final ObjectMapper serializer = new ObjectMapper();

    public Message() {
    }

    public Message(Message other) {
        content = new HashMap<>();
        content.putAll(other.content);
        for (String category : other.content.keySet()) {
            content.put(category, new HashMap<>());
            content.get(category).putAll(other.content.get(category));
        }
    }

    public Message addWithCategory(String category, String key, String value) {
        // Erstelle die Kategorie, falls sie nicht existiert
        if (!content.containsKey(category)) {
            content.put(category, new HashMap<>());
        }
        content.get(category).put(key, value);
        return this;
    }

    public Message add(String key, String value) {
        return addWithCategory("Payload", key, value);
    }

    public Message add(String key, int value) {
        return addWithCategory("Payload", key, String.valueOf(value));
    }

    public Message add(String key, float value) {
        return addWithCategory("Payload", key, String.valueOf(value));
    }

    public Message add(String key, List<Double> value) {
        return addWithCategory("Payload", key, value.toString());
    }

    public Message addHeader(String key, String value) {
        return addWithCategory("Header", key, value);
    }

    public Message addHeader(String key, int value) {
        return addWithCategory("Header", key, String.valueOf(value));
    }

    public String queryWithCategory(String category, String key) {
        Map<String, String> categoryMap = content.get(category);
        if (categoryMap == null) {
            return null;
        }
        return categoryMap.get(key);
    }

    public String query(String key) {
        return queryWithCategory("Payload", key);
    }

    public int queryInteger(String key) {
        return Integer.parseInt(queryWithCategory("Payload", key));
    }

    public float queryFloat(String key) {
        return Float.parseFloat(queryWithCategory("Payload", key));
    }

    public List<Double> queryDoubleArray(String key) {

        String value = queryWithCategory("Payload", key);

        if (value == null || value.trim().equals("[]") || value.trim().isEmpty()) {
            return new ArrayList<Double>();
        }

        String[] stringArray = value.replaceAll("[\\[\\]]", "").split(", ");
        List<Double> DoubleArray = new ArrayList<Double>();
        for (int i = 0; i < stringArray.length; i++) {
            if (!stringArray[i].trim().isEmpty()) {
                DoubleArray.add(Double.parseDouble(stringArray[i].trim()));
            }
        }
        return DoubleArray;
    }

    public String queryHeader(String key) {
        return queryWithCategory("Header", key);
    }

    public int queryHeaderInteger(String key) {
        return Integer.parseInt(queryWithCategory("Header", key));
    }

    public Map<String, String> getPayload() {
        return content.get("Payload");
    }

    public Map<String, String> getHeader() {
        return content.get("Header");
    }

    public String toJson() throws JsonProcessingException {
        return serializer.writeValueAsString(this);
    }

    public static Message fromJson(String s) throws IOException {
        return serializer.readValue(s, Message.class);
    }

    @Override
    public String toString() {
        String result;
        try {
            result = toJson();
        } catch (JsonProcessingException e) {
            result = "Unable to serialize message";
        }
        return result;
    }

}
