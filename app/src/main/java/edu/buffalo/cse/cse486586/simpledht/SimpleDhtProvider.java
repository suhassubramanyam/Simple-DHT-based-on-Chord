package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static final String KEYVALUE_TABLE_NAME = "dht";
    private static final int SERVER_PORT = 10000;
    private static final String MSG_REQUEST_TYPE = "MSG_REQUEST_TYPE";
    private static final String SENDER_PORT = "SENDER_PORT";
    private static final String SUCCESSOR = "SUCCESSOR";
    private static final String PREDECESSOR = "PREDECESSOR";

    private static final String FIRST_NODE_JOIN = "FIRST_NODE_JOIN";
    private static final String INSERT = "INSERT";
    private static final String QUERY_ALL = "QUERY_ALL";
    private static final String QUERY_ONLY = "QUERY_ONLY";
    private static final String UPDATE_BOTH = "UPDATE_BOTH";
    private static final String UPDATE_PRED = "UPDATE_PRED";
    private static final String UPDATE_SUCC = "UPDATE_SUCC";
    private static final String NEXT_NODE_JOIN = "NEXT_NODE_JOIN";
    private static final String FORWARDING_PORT = "FORWARDING_PORT";
    private static final String MIN_HASH = "0000000000000000000000000000000000000000";
    private static final String MAX_HASH = "ffffffffffffffffffffffffffffffffffffffff";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String STAR_SIGN = "*";
    private static final String AT_SIGN = "@";
    private static final String PROVIDER_URI = "content://edu.buffalo.cse.cse486586.simpledht.provider";

    private SQLiteDatabase database;

    private static String serverPort;
    private static String node_id;
    private static String predPort;
    private static String succPort;


    private Executor myExec = Executors.newSingleThreadExecutor();

    public static class KeyValueOpenHelper extends SQLiteOpenHelper {

        private static final String DATABASE_NAME = "PA3";
        private static final int DATABASE_VERSION = 2;

        private static final String KEY = "key";
        private static final String VALUE = "value";
        private static final String KEYVALUE_TABLE_CREATE =
                "CREATE TABLE " + KEYVALUE_TABLE_NAME + " (" +
                        KEY + " TEXT PRIMARY KEY, " +
                        VALUE + " TEXT);";

        KeyValueOpenHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("DROP TABLE IF EXISTS " + KEYVALUE_TABLE_NAME);
            db.execSQL(KEYVALUE_TABLE_CREATE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int a, int b) {
            //Do nothing
        }
    }

    private void sendMsgUsingSocket(String m, String port) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        out.write(m);
        out.close();
        socket.close();
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        Log.d(TAG, "delete: uri: " + uri + " selection: " + selection);

        if (STAR_SIGN.equals(selection) || AT_SIGN.equals(selection))
            database.execSQL("DELETE FROM " + KEYVALUE_TABLE_NAME);
        else
            database.execSQL("DELETE FROM " + KEYVALUE_TABLE_NAME + " WHERE key=" + "'" + selection + "'");
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(TAG, "insert: " + "ContentValues: " + values + ", " + "Uri: " + uri.toString());

        String key = values.getAsString("key");
        String value = values.getAsString("value");

        Log.d(TAG, "insert: Got Key: " + key + " value: " + value);

        try {
            String hKey = genHash(key);
            String hPredID = genHash(getIDfromPort(predPort));
            String hServerID = genHash(getIDfromPort(serverPort));

            if (hPredID.equals(hServerID)||(hKey.compareTo(hPredID) > 0 && hKey.compareTo(hServerID) < 0)
                    || (hKey.compareTo(hPredID) > 0 && hKey.compareTo(MAX_HASH) <= 0 && hPredID.compareTo(hServerID) > 0)
                    || (hKey.compareTo(MIN_HASH) >= 0 && hKey.compareTo(hServerID) < 0) && hPredID.compareTo(hServerID) > 0){

                Log.d(TAG, "insert: Should be stored in current AVD");

                long row = database.insertWithOnConflict(KEYVALUE_TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.d(TAG, "insert: row: " + row);
                Uri newUri = uri;
                if (row > 0) {
                    newUri = ContentUris.withAppendedId(uri, row);
                    if (getContext() != null) {
                        getContext().getContentResolver().notifyChange(newUri, null);
                    }
                }

                return newUri;

            }
            else{
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(MSG_REQUEST_TYPE,INSERT);
                jsonObject.put(KEY,key);
                jsonObject.put(VALUE,value);
                jsonObject.put(FORWARDING_PORT,succPort);

                new ClientTask().executeOnExecutor(myExec, jsonObject.toString());
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }

      return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        if (getContext() != null) {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            serverPort = String.valueOf(Integer.parseInt(portStr) * 2);
            predPort = serverPort;
            succPort = serverPort;
            Log.d(TAG, "onCreate: serverPort : " + serverPort);
            Log.d(TAG, "onCreate: predPort : "+predPort);
            Log.d(TAG, "onCreate: succPort : "+succPort);

            try {
                node_id = genHash(portStr);
                Log.d(TAG, "onCreate: Setting node_id: " + node_id);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if (!serverPort.equals("11108")) {
                JSONObject jsonObject = new JSONObject();
                try {
                    jsonObject.put(MSG_REQUEST_TYPE, FIRST_NODE_JOIN);
                    jsonObject.put(SENDER_PORT, serverPort);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                Log.d(TAG, "onCreate: jsonObject: " + jsonObject);
                new ClientTask().executeOnExecutor(myExec, jsonObject.toString());
            }
            try {

                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
            }
        }
        Context context = getContext();
        KeyValueOpenHelper kVHelper = new KeyValueOpenHelper(context);
        database = kVHelper.getWritableDatabase();

        return database != null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        if(AT_SIGN.equals(selection)) {
            Log.d(TAG, "query: Reached AT_SIGN: "+serverPort);
            return myQuery(uri, selection);
        } else if(STAR_SIGN.equals(selection)){
            Log.d(TAG, "query: Reached STAR_SIGN: "+serverPort);
            try {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(KEY, selection);
                jsonObject.put(MSG_REQUEST_TYPE, QUERY_ALL);
                jsonObject.put(FORWARDING_PORT, succPort);
                jsonObject.put(SENDER_PORT, serverPort);
                Log.d(TAG, "query: Executing client task");
                AsyncTask<String,String,String> a =new ClientTask();
                a.executeOnExecutor(myExec, jsonObject.toString());
                Log.d(TAG, "query: "+ a.getStatus());
                String s = a.get();
                Log.d(TAG, "query: "+ a.getStatus());
                Log.d(TAG, "query: a.get(): "+s);
                return jsonArr2MatrixCursor(new JSONArray(s));
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else{
            Log.d(TAG, "query: Reached ELSE case: "+serverPort);
            try {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(KEY, selection);
                jsonObject.put(MSG_REQUEST_TYPE, QUERY_ONLY);
                jsonObject.put(FORWARDING_PORT, succPort);
                jsonObject.put(SENDER_PORT, serverPort);
                Cursor cursor = myQuery(uri,selection);
                Log.d(TAG, "query: Executing client task");
                AsyncTask<String,String,String> a =new ClientTask();
                a.executeOnExecutor(myExec, jsonObject.toString());
                Log.d(TAG, "query: "+ a.getStatus());
                String s = a.get();
                Log.d(TAG, "query: "+ a.getStatus());
                Log.d(TAG, "query: a.get(): "+s);
                return jsonArr2MatrixCursor(new JSONArray(s));
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        return myQuery(uri,selection);
    }

    private Cursor myQuery(Uri uri,String selection){
        Log.d(TAG, "myQuery: " + "Uri: " + uri + ", " + "selection: " + selection);
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(KEYVALUE_TABLE_NAME);

        if (STAR_SIGN.equals(selection) || AT_SIGN.equals(selection))
            selection = "1==1";
        else
            selection = "key=" + "'" + selection + "'"; // appending the key sent to the Where clause

        Cursor cursor = queryBuilder.query(database, null, selection, null, null, null, null);
        if (getContext() != null) {
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
        }
        Log.d(TAG, "myQuery: cursor: " + cursor);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private JSONArray cur2Json(Cursor cursor) {

        JSONArray resultSet = new JSONArray();
        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            int totalColumn = cursor.getColumnCount();
            JSONObject rowObject = new JSONObject();
            for (int i = 0; i < totalColumn; i++) {
                if (cursor.getColumnName(i) != null) {
                    try {
                        rowObject.put(cursor.getColumnName(i),
                                cursor.getString(i));
                    } catch (Exception e) {
                        Log.d(TAG, e.getMessage());
                    }
                }
            }
            resultSet.put(rowObject);
            cursor.moveToNext();
        }

        //cursor.close();
        return resultSet;

    }

    private JSONArray concatArray(JSONArray arr1, JSONArray arr2)
            throws JSONException {
        JSONArray result = new JSONArray();
        for (int i = 0; i < arr1.length(); i++) {
            result.put(arr1.get(i));
        }
        for (int i = 0; i < arr2.length(); i++) {
            result.put(arr2.get(i));
        }
        return result;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getIDfromPort(String s){
        return String.valueOf(Integer.parseInt(s) / 2);
    }

    private MatrixCursor jsonArr2MatrixCursor(JSONArray jsonArray){
        String[] columnNames = {"key","value"};
        MatrixCursor mc = new MatrixCursor(columnNames);

            try {
                for(int i=0 ; i < jsonArray.length(); i++ ) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    mc.addRow(new String[]{jsonObject.getString("key"),jsonObject.getString("value")});
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }


        return mc;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private final String TAG = this.getClass().getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String m = input.readLine();

                    JSONObject msgJsonObj = new JSONObject(m);
                    Log.d(TAG, "*******************NEW REQUEST************************");
                    Log.d(TAG, "doInBackground: received msgJsonObj: "+msgJsonObj);

                    String msg_request_type = msgJsonObj.getString(MSG_REQUEST_TYPE);
                    Log.d(TAG, "doInBackground: msg_request_type: "+msg_request_type);




                    if (msg_request_type.equals(FIRST_NODE_JOIN) || msg_request_type.equals(NEXT_NODE_JOIN)) {


                        String senderPort = msgJsonObj.getString(SENDER_PORT);
                        Log.d(TAG, "doInBackground: senderPort: "+senderPort);

                        String hSenderID = genHash(getIDfromPort(senderPort));
                        String hPredID = genHash(getIDfromPort(predPort));
                        String hSuccID = genHash(getIDfromPort(succPort));
                        String hServerID = genHash(getIDfromPort(serverPort));

                        Log.d(TAG, "doInBackground: senderPort: "+senderPort+" predPort: "+predPort+" succPort: "+succPort+" serverPort: "+serverPort);
                        Log.d(TAG, "doInBackground: hSenderID: " + hSenderID + " hPredID: " + hPredID + " hSuccID: " + hSuccID + " hServerID: " + hServerID);

                        Log.d(TAG, "doInBackground: This is the FIRST_NODE_JOIN_REQUEST");

                        //Case when only 5554 is there and new node is planning to join
                        if (hPredID.equals(hSuccID) && hPredID.equals(hServerID)) {
                            Log.d(TAG, "doInBackground: Received the first request to join node, senderPort: " + senderPort);

                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(MSG_REQUEST_TYPE, UPDATE_BOTH);
                            jsonObject.put(PREDECESSOR, serverPort);
                            jsonObject.put(SUCCESSOR, serverPort);
                            jsonObject.put(SENDER_PORT, senderPort);
                            Log.d(TAG, "doInBackground: jsonObject: " + jsonObject);

                            predPort = senderPort;
                            succPort = senderPort;

                            publishProgress(jsonObject.toString());

                        } else if ((hSenderID.compareTo(hPredID) > 0 && hSenderID.compareTo(hServerID) < 0)
                                  || (hSenderID.compareTo(hPredID) > 0 && hSenderID.compareTo(MAX_HASH) <= 0 && hPredID.compareTo(hServerID) > 0)
                                  || (hSenderID.compareTo(MIN_HASH) >= 0 && hSenderID.compareTo(hServerID) < 0) && hPredID.compareTo(hServerID) > 0) {

                            Log.d(TAG, "doInBackground: Received request for adding node in between: " + predPort + " and " + serverPort + " for " + senderPort);

                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(MSG_REQUEST_TYPE, UPDATE_BOTH);
                            jsonObject.put(PREDECESSOR, predPort);
                            jsonObject.put(SUCCESSOR, serverPort);
                            jsonObject.put(SENDER_PORT, senderPort);
                            Log.d(TAG, "doInBackground: jsonObject: " + jsonObject);


                            JSONObject jsonObject1 = new JSONObject();
                            jsonObject1.put(MSG_REQUEST_TYPE, UPDATE_SUCC);
                            jsonObject1.put(SUCCESSOR, senderPort);
                            jsonObject1.put(SENDER_PORT, predPort);
                            Log.d(TAG, "doInBackground: jsonObject1: " + jsonObject1);

                            predPort = senderPort;
                            Log.d(TAG, "doInBackground: Setting predPort: " + predPort);

                            publishProgress(jsonObject.toString());
                            publishProgress(jsonObject1.toString());

                        }
                        else {
                            Log.d(TAG, "doInBackground: Forwarding the NODE_JOIN request to succPort: " + succPort);

                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(MSG_REQUEST_TYPE,NEXT_NODE_JOIN);
                            jsonObject.put(SENDER_PORT,senderPort);
                            jsonObject.put(FORWARDING_PORT,succPort);
                            Log.d(TAG, "doInBackground: jsonObject: " + jsonObject);
                            publishProgress(jsonObject.toString());

                        }
                    } else if (msg_request_type.equals(UPDATE_BOTH)) {

                        predPort = msgJsonObj.getString(PREDECESSOR);
                        succPort = msgJsonObj.getString(SUCCESSOR);
                        Log.d(TAG, "doInBackground: Updating predPort to: " + predPort + " succPort: " + succPort);

                    } else if (msg_request_type.equals(UPDATE_PRED)) {

                        predPort = msgJsonObj.getString(PREDECESSOR);
                        Log.d(TAG, "doInBackground: Updating predPort to: " + predPort);

                    } else if (msg_request_type.equals(UPDATE_SUCC)) {

                        succPort = msgJsonObj.getString(SUCCESSOR);
                        Log.d(TAG, "doInBackground: Updating succPort: " + succPort);

                    } else if(msg_request_type.equals(INSERT)){
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key",msgJsonObj.getString(KEY));
                        contentValues.put("value",msgJsonObj.getString(VALUE));

                        insert(Uri.parse(PROVIDER_URI), contentValues);
                    } else if(QUERY_ALL.equals(msg_request_type)){
                        Log.d(TAG, "doInBackground: ServerTask: "+serverPort);

                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put(KEY,msgJsonObj.getString(KEY));
                        jsonObject.put(MSG_REQUEST_TYPE, QUERY_ALL);
                        jsonObject.put(FORWARDING_PORT, succPort);
                        jsonObject.put(SENDER_PORT,msgJsonObj.getString(SENDER_PORT));

                        if(!succPort.equals(msgJsonObj.getString(SENDER_PORT))) { //If we haven't reached the end of the ring

                            Log.d(TAG, "doInBackground: Inside IF condition");  
                            Socket socketForw = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                            PrintWriter out = new PrintWriter(socketForw.getOutputStream(), true);
                            out.println(jsonObject);
                            Log.d(TAG, "doInBackground: Writing "+jsonObject+" to "+jsonObject.getString(FORWARDING_PORT));
                            
                            
                            BufferedReader retInput = new BufferedReader(new InputStreamReader(socketForw.getInputStream()));
                            String retValue = retInput.readLine();
                            Log.d(TAG, "doInBackground: Read value: "+retValue);
                            JSONArray jsonArray = new JSONArray(retValue);
                            Log.d(TAG, "doInBackground: Converting to jsonArray: "+jsonArray);

                            Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),msgJsonObj.getString(KEY));
                            JSONArray jsonArray1 = cur2Json(cursor);
                            Log.d(TAG, "doInBackground: Local Query JSONArray: "+jsonArray1);

                            JSONArray jsonArray2 = concatArray(jsonArray,jsonArray1);
                            Log.d(TAG, "doInBackground: mergedJSONArray: "+jsonArray2);

                            PrintWriter retOutput = new PrintWriter(socket.getOutputStream(), true);
                            retOutput.println(jsonArray2);
                            Log.d(TAG, "doInBackground: Writing back from: "+serverPort);

                        } else {
                            Log.d(TAG, "doInBackground: Inside ELSE condition");
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),msgJsonObj.getString(KEY));
                            Log.d(TAG, "doInBackground: JSON_ARRAY: "+cur2Json(cursor).toString());
                            out.println(cur2Json(cursor).toString());

                            Log.d(TAG, "doInBackground: Writing back from: "+serverPort);
                        }

                    } else if(QUERY_ONLY.equals(msg_request_type)){
                        Log.d(TAG, "doInBackground: ServerTask: "+serverPort);

                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put(KEY,msgJsonObj.getString(KEY));
                        jsonObject.put(MSG_REQUEST_TYPE, QUERY_ONLY);
                        jsonObject.put(FORWARDING_PORT, succPort);
                        jsonObject.put(SENDER_PORT,msgJsonObj.getString(SENDER_PORT));

                        Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),msgJsonObj.getString(KEY));
                        if(cursor.getCount() <= 0) { //If we haven't reached the end of the ring

                            Log.d(TAG, "doInBackground: Inside IF condition");
                            Socket socketForw = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                            PrintWriter out = new PrintWriter(socketForw.getOutputStream(), true);
                            out.println(jsonObject);
                            Log.d(TAG, "doInBackground: Writing "+jsonObject+" to "+jsonObject.getString(FORWARDING_PORT));


                            BufferedReader retInput = new BufferedReader(new InputStreamReader(socketForw.getInputStream()));
                            String retValue = retInput.readLine();
                            Log.d(TAG, "doInBackground: Read value: "+retValue);
                            JSONArray jsonArray = new JSONArray(retValue);
                            Log.d(TAG, "doInBackground: Converting to jsonArray: "+jsonArray);

                            PrintWriter retOutput = new PrintWriter(socket.getOutputStream(), true);
                            retOutput.println(jsonArray.toString());
                            Log.d(TAG, "doInBackground: Writing back from: "+serverPort);

                        } else { //Key found
                            Log.d(TAG, "doInBackground: Inside ELSE condition, Key Found");
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            Log.d(TAG, "doInBackground: JSON_ARRAY: "+cur2Json(cursor).toString());
                            out.println(cur2Json(cursor).toString());

                            Log.d(TAG, "doInBackground: Writing back from: "+serverPort);
                        }
                    }

                } catch (IOException e) {
                    Log.d(TAG, e.toString());
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

            //return null;
        }

        protected void onProgressUpdate(String... msg) {
            /*
             * The following code displays what is received in doInBackground().
             */
            if(msg.length == 1) {
                String m = msg[0];
                Log.d(TAG, "onProgressUpdate: String: " + m);
                new ClientTask().executeOnExecutor(myExec, m);
            }


        }
    }

    private class ClientTask extends AsyncTask<String, String, String> {
        private final String TAG = this.getClass().getSimpleName();

        @Override
        protected String doInBackground(String... msgs) {
            String m = msgs[0];
            Log.d(TAG, "doInBackground: This is client " + m);
            try {
                JSONObject jsonObject = new JSONObject(m);
                String request_type = jsonObject.getString(MSG_REQUEST_TYPE);

                if (request_type.equals(FIRST_NODE_JOIN)) {
                    Log.d(TAG, "doInBackground: Sending FIRST_NODE_JOIN to 11108");
                    sendMsgUsingSocket(m, "11108");

                } else if (request_type.equals(UPDATE_BOTH) || request_type.equals(UPDATE_PRED)
                        || request_type.equals(UPDATE_SUCC)) {

                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(SENDER_PORT));
                    sendMsgUsingSocket(m, jsonObject.getString(SENDER_PORT));

                } else if(request_type.equals(NEXT_NODE_JOIN)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));
                    sendMsgUsingSocket(m,jsonObject.getString(FORWARDING_PORT));
                } else if(request_type.equals(INSERT)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));
                    sendMsgUsingSocket(m,jsonObject.getString(FORWARDING_PORT));
                } else if(QUERY_ALL.equals(request_type)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(m);

                    Log.d(TAG, "doInBackground: Writing "+m+" to "+jsonObject.getString(FORWARDING_PORT));



                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String retValue = input.readLine();
                    Log.d(TAG, "doInBackground: Returned value from Server [" + jsonObject.getString(FORWARDING_PORT) + "]" + " is: " + retValue);
                    //input.close();
                    //socket.close();
                    JSONArray jsonArray = new JSONArray(retValue);
                    Log.d(TAG, "doInBackground: Converting to jsonArray: "+jsonArray);

                    Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),new JSONObject(m).getString(KEY));
                    JSONArray jsonArray1 = cur2Json(cursor);
                    Log.d(TAG, "doInBackground: Local Query JSONArray: "+jsonArray1);

                    JSONArray jsonArray2 = concatArray(jsonArray,jsonArray1);
                    Log.d(TAG, "doInBackground: mergedJSONArray: "+jsonArray2);
                    return jsonArray2.toString();
                } else if(QUERY_ONLY.equals(request_type)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));

                    Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),new JSONObject(m).getString(KEY));
                    if(cursor.getCount() <= 0) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(m);

                        Log.d(TAG, "doInBackground: Writing " + m + " to " + jsonObject.getString(FORWARDING_PORT));


                        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String retValue = input.readLine();
                        Log.d(TAG, "doInBackground: Returned value from Server [" + jsonObject.getString(FORWARDING_PORT) + "]" + " is: " + retValue);
                        return retValue;
                    }
                    return cur2Json(cursor).toString();
                }


            } catch (JSONException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}
