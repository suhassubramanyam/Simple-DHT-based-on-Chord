package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

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
    private static final String QUERY = "QUERY";
    private static final String UPDATE_BOTH = "UPDATE_BOTH";
    private static final String UPDATE_PRED = "UPDATE_PRED";
    private static final String UPDATE_SUCC = "UPDATE_SUCC";
    private static final String NEXT_NODE_JOIN = "NEXT_NODE_JOIN";
    private static final String FORWARDING_PORT = "FORWARDING_PORT";
    private static final String MIN_HASH = "0000000000000000000000000000000000000000";
    private static final String MAX_HASH = "ffffffffffffffffffffffffffffffffffffffff";


    private SQLiteDatabase database;

    private static String serverPort;
    private static String node_id;
    private static String predPort;
    private static String succPort;

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

        if (selection.equalsIgnoreCase("*") || selection.equalsIgnoreCase("@"))
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
        try {
            String hKey = genHash(key);
            String hPredPort = genHash(getIDfromPort(predPort));
            String hSuccPort = genHash(getIDfromPort(succPort));
            String hServerPort = genHash(getIDfromPort(serverPort));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
              //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

        long row = database.insertWithOnConflict(KEYVALUE_TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
        Log.d(TAG, "insert: row: " + row);
        Uri newUri = uri;
        if (row > 0) {
            newUri = ContentUris.withAppendedId(uri, row);
            if (getContext() != null) {
                getContext().getContentResolver().notifyChange(newUri, null);
            }
        }
        Log.d(TAG, "insert: newUri: " + newUri);
        return newUri;
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
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, jsonObject.toString());
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
        // TODO Auto-generated method stub
        Log.d(TAG, "query: " + "Uri: " + uri + ", " + "selection: " + selection);
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(KEYVALUE_TABLE_NAME);

        if (selection.equalsIgnoreCase("*") || selection.equalsIgnoreCase("@"))
            selection = "1==1";
        else
            selection = "key=" + "'" + selection + "'"; // appending the key sent to the Where clause

        Cursor cursor = queryBuilder.query(database, projection, selection, selectionArgs, null, null, sortOrder);
        if (getContext() != null) {
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
        }
        Log.d(TAG, "query: cursor: " + cursor);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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
        return String.valueOf(Integer.parseInt(s)/2);
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private final String TAG = this.getClass().getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket s = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String m = input.readLine();

                    JSONObject msgJsonObj = new JSONObject(m);
                    Log.d(TAG, "*******************NEW REQUEST************************");
                    Log.d(TAG, "doInBackground: received msgJsonObj: "+msgJsonObj);

                    String msg_request_type = msgJsonObj.getString(MSG_REQUEST_TYPE);
                    Log.d(TAG, "doInBackground: msg_request_type: "+msg_request_type);

                    String senderPort = msgJsonObj.getString(SENDER_PORT);
                    Log.d(TAG, "doInBackground: senderPort: "+senderPort);

                     /*String hSenderPort = genHash(senderPort);
                    String hPredPort = genHash(predPort);
                    String hSuccPort = genHash(succPort);
                    String hServerPort = genHash(serverPort);
                    */
                    String hSenderID = genHash(getIDfromPort(senderPort));
                    String hPredID = genHash(getIDfromPort(predPort));
                    String hSuccID = genHash(getIDfromPort(succPort));
                    String hServerID = genHash(getIDfromPort(serverPort));

                    Log.d(TAG, "doInBackground: senderPort: "+senderPort+" predPort: "+predPort+" succPort: "+succPort+" serverPort: "+serverPort);
                    Log.d(TAG, "doInBackground: hSenderID: " + hSenderID + " hPredID: " + hPredID + " hSuccID: " + hSuccID + " hServerID: " + hServerID);

                    if (msg_request_type.equals(FIRST_NODE_JOIN) || msg_request_type.equals(NEXT_NODE_JOIN)) {
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
            String m = msg[0];
            Log.d(TAG, "onProgressUpdate: String: " + m);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

        }
    }

    private class ClientTask extends AsyncTask<String, String, Void> {
        private final String TAG = this.getClass().getSimpleName();

        @Override
        protected Void doInBackground(String... msgs) {
            Log.d(TAG, "doInBackground: msgs: " + Arrays.toString(msgs));
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

                }else if(request_type.equals(NEXT_NODE_JOIN)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));
                    sendMsgUsingSocket(m,jsonObject.getString(FORWARDING_PORT));
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
