package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {
    private Uri mUri;
    public String myPort;
    public String sucId;
    public String hashedPredId;
    public String hashedSucId;
    public String nodeId;
    public String predId;
    public List<String> listOfLocalInsertedKeys = new ArrayList<String>();
    public List<String> listOfRemoteKeys = new ArrayList<String>();
    public Set<String> insertionSuccessRelayed = new HashSet<String>();
    public Set<String> deletionSuccessRelayed = new HashSet<String>();
    public List<String> sortedPortHashValues = new ArrayList<String>();
    public Map<String, String> portHash = new HashMap<String, String>();
    public Map<String, String> queryResult = new HashMap<String, String>();
    static final int SERVER_PORT = 10000;
    public List<String> joinedPorts = new ArrayList<String>();
    String colNames[] = {"key", "value"};


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        // The following code implements the delete functionality
        // If the selection string is @, remove all the local keys and notify other AVDs to remove
        // the keys from their remote key list
        if (selection.equals("@")) {
            for (String key : listOfLocalInsertedKeys) {
                String requestDelete = "D:" + key;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestDelete, sucId);
            }
            listOfLocalInsertedKeys.clear();
        }
        // If the selection string is *, remove all the local keys and remote keys
        // and notify other AVDs to remove all the keys
        else if (selection.equals("*")) {
            for (String key : listOfLocalInsertedKeys) {
                String requestDelete = "D:" + key;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestDelete, sucId);
            }
            listOfLocalInsertedKeys.clear();
            for (String key : listOfRemoteKeys) {
                String requestDelete = "D:" + key;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestDelete, sucId);
            }
            listOfRemoteKeys.clear();
        }
        // If the selection string is the key, remove the key locally and inform other AVDs to remove
        // from their remote keys
        else {
            String requestDelete = "D:" + selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestDelete, sucId);
            if (listOfLocalInsertedKeys.contains(selection))
                listOfLocalInsertedKeys.remove(selection);
            if (listOfRemoteKeys.contains(selection)) listOfRemoteKeys.remove(selection);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // The following code implements the insert functionality
        // Retrieve the key-value pairs
        // Reference : PA2A Code
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        String keyHash = null;
        try {
            // Generate the SHA-1 hash of the key
            // Reference : PA3
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.d("Insert Q", keyHash);
        // The following conditions are checked to insert in the local AVD
        // Check if the key belongs to the local AVD, if not, forward the insert request to the successor node
        // 1. hashedPredId == null, to check if the AVD is the only AVD in the ring, then insert locally
        // 2. If the key hash is between predecessor node id and local node id
        // 3. If this is the first node in the ring and the key is larger than last node id
        // 4. If this is the first node in the ring and the key is smaller than last node id
        if ((hashedPredId == null) ||
                (keyHash.compareTo(hashedPredId) > 0 && keyHash.compareTo(nodeId) <= 0) ||
                (keyHash.compareTo(hashedPredId) > 0 && keyHash.compareTo(nodeId) >= 0 && hashedPredId.compareTo(nodeId) >= 0) ||
                (keyHash.compareTo(hashedPredId) < 0 && keyHash.compareTo(nodeId) < 0 && hashedPredId.compareTo(nodeId) >= 0)) {
            Log.d("Inserting KV in this", key + ":" + value);
            listOfLocalInsertedKeys.add(key);
            insertIntoFile(key, value);
            String insertSuccess = "IS:" + key + ":" + value;
            if (hashedPredId != null && hashedPredId.compareTo(nodeId) != 0)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertSuccess, myPort);
        } else {
            // If key does not belong to this AVD, forward it to the successor node
            String insertQuery = "I:" + key + ":" + value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertQuery, sucId);
        }
        return uri;
    }

    private synchronized void insertIntoFile(String key, String value) {
        /*
         * The following code sets the filename of the file as the key and the content of the file
         * as the value and writes to the file
         * Reference :
         * https://developer.android.com/training/data-storage/files#WriteInternalStorage
         * PA1 Code
         */
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            Log.d("Insert Success", key + ":" + value);
        } catch (Exception e) {
            Log.e("FileOutputCreation", "Error");
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        // Do the port set up and initiate the node join requests when the Content Provider is
        // created
        try {
            setUpPortAndServer();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        initiateNodeJoin();
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.d("Query function called", selection);
        MatrixCursor cursor = new MatrixCursor(colNames);
        // TODO Auto-generated method stub
        // If the selection string is @, retrieve all locally stored key-value pairs
        if (selection.equals("@")) {
            for (String key : listOfLocalInsertedKeys) {
                String value = retrieveFromFile(key);
                String res[] = {key, value};
                cursor.addRow(res);
                Log.d("Retrieved", key + ":" + value);
            }
        } else if (selection.equals("*")) {
            // If the selection string is *, retrive all local and remote key-value pairs
            // Retrieve locally stored key value pairs
            for (String key : listOfLocalInsertedKeys) {
                String value = retrieveFromFile(key);
                String res[] = {key, value};
                cursor.addRow(res);
                Log.d("Retrieved", key + ":" + value);
            }
            // Retrieve remote located key value pairs - Send a query
            // request to successor nodes querying for each key and wait for result to come back

            for (String remoteKey : listOfRemoteKeys) {
                String queryRequest = "QR:" + remoteKey + ":" + myPort;
                queryResult.put(remoteKey, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryRequest, sucId);
                while (queryResult.get(remoteKey) == null) {
                    // Wait for result to come back --- Blocking call
                }
                if (queryResult.get(remoteKey) != null) {
                    String res[] = {remoteKey, queryResult.get(remoteKey)};
                    cursor.addRow(res);
                    Log.d("Retrieved", remoteKey + ":" + queryResult.get(remoteKey));
                }

            }
        } else {
            // If the selection string is the key itself, check local AVD if the key exists,
            // else send the query request to successor node and wait for result to come back
            if (listOfLocalInsertedKeys.contains(selection)) {
                String value = retrieveFromFile(selection);
                String res[] = {selection, value};
                cursor.addRow(res);
            } else {
                String queryRequest = "QR:" + selection + ":" + myPort;
                queryResult.put(selection, null);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryRequest, sucId);
                while (queryResult.get(selection) == null) {
                    // Wait for result to come back --- Blocking call
                }
                if (queryResult.get(selection) != null) {
                    String res[] = {selection, queryResult.get(selection)};
                    cursor.addRow(res);
                }
            }
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String retrieveFromFile(String key) {
        /*
         * The following code reads the content of the file and creates a cursor by setting key as
         * filename and content of the file as the value
         * Reference :
         * https://developer.android.com/reference/android/database/MatrixCursor#addRow(java.lang.Object[])
         */
        FileInputStream inputStream;
        String value = null;
        byte[] bArray = new byte[128];
        try {
            inputStream = getContext().openFileInput(key);
            int readCount = inputStream.read(bArray);
            if (readCount != -1) value = new String(bArray, 0, readCount);
            inputStream.close();
        } catch (IOException e) {
            Log.e("File read Error", key);
        }
        return value;
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

    private void setUpPortAndServer() throws NoSuchAlgorithmException, IOException {

        /*
         * Calculate the port number that this AVD listens on.
         * Reference : PA1 Code
         */
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        // Generate the node id from emulator port,
        // e.g. If emulator port = 11108, node id = genHash(11108/2), i.e. genHash(5554)
        nodeId = genHash(String.valueOf(Integer.parseInt(myPort) / 2));
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        /*
         * Create a server socket as well as a thread (AsyncTask) that listens on the server
         * port.
         *
         * AsyncTask is a simplified thread construct that Android provides.
         * http://developer.android.com/reference/android/os/AsyncTask.html
         * Reference : PA1
         */
        ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);


    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private synchronized void initiateNodeJoin() {
        // Create the node join request by appending the joiner's port and sending it to 11108

        String nodeJoin = "JR:" + myPort;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeJoin, "11108");
        // Note : In case of only a single AVD, the node join fails
        // and raises an exception, this causes predecessor/succ node values to become null,
        // this causes the AVD to store all the key values locally, which
        // takes care of the condition of single node chord ring
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket clientConnection = null;
            DataInputStream dataInputStream = null;
            DataOutputStream dataOutputStream = null;
            try {
                while (true) {
                    /*
                     * The following code listens for incoming connections and accepts if a connection attempt is made
                     * and reads the received string data and sends
                     * then sends an acknowledgement back to the client
                     * References :
                     * 1. https://stackoverflow.com/questions/7384678/how-to-create-socket-connection-in-android
                     * 2. https://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
                     * 3. https://developer.android.com/reference/android/os/AsyncTask
                     * 4. PA1 Code
                     */

                    clientConnection = serverSocket.accept();
                    dataInputStream = new DataInputStream(clientConnection.getInputStream());
                    String readString = dataInputStream.readUTF();
                    Log.d("Server read", readString);
                    String ack = "Server Ack";
                    dataOutputStream = new DataOutputStream(clientConnection.getOutputStream());
                    dataOutputStream.writeUTF(ack);
                    processMessage(readString);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (clientConnection != null) clientConnection.close();
                    if (dataInputStream != null) dataInputStream.close();
                    if (dataOutputStream != null) dataOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            return;
        }

        public void processMessage(String message) {
            // The following code processes the message at the server
            String[] parts = message.split(":");
            String messageType = parts[0];
            if (messageType.equals("JR")) {
                String joinRequestFrom = parts[1];
                try {
                    processJoinRequest(joinRequestFrom);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            } else if (messageType.equals("JA")) {
                processJoinAcceptance(parts[1], parts[2]);
            } else if (messageType.equals("I")) {
                processInsertRequest(parts[1], parts[2]);
            } else if (messageType.equals("IS")) {
                processInsertSuccess(message, parts[1]);
            } else if (messageType.equals("QR")) {
                String queryKey = parts[1];
                String queryRequestFrom = parts[2];
                processQueryRequest(message, queryKey, queryRequestFrom);
            } else if (messageType.equals("QA")) {
                String resultKey = parts[1];
                String resultValue = parts[2];
                processQueryAnswer(resultKey, resultValue);
            } else if (messageType.equals("D")) {
                String keyToDelete = parts[1];
                processDelete(message, keyToDelete);
            }
        }

        private void updatePredSuc(String pred, String suc) {
            /*
             * The following code updates the pred/suc of the AVD and calculates the hash of them
             * respectively in hashedPredId and hashedSucId
             */
            predId = pred;
            sucId = suc;
            try {
                hashedPredId = genHash(String.valueOf(Integer.parseInt(predId) / 2));
                hashedSucId = genHash(String.valueOf(Integer.parseInt(sucId) / 2));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.d("Update Pre,Suc", predId + ":" + sucId);
            Log.d("Hashes", hashedPredId + ":" + nodeId + ":" + hashedSucId);

        }

        public void processJoinRequest(String joinRequestFrom) throws NoSuchAlgorithmException {
            /*
             * This code is for emulator 5554, when it receives a join request, it calculates
             * the hash of the port and maintains the list of joined port hashes in a sorted manner
             * This is used to correctly calculate the predecessor and successor nodes for each
             * of the nodes that have joined. After correctly calculating the pred/suc for each
             * AVD, it notifies the pred and successor nodes for each AVD by sending joinAcceptance
             */
            String joinRequestNodeId = String.valueOf(Integer.parseInt(joinRequestFrom) / 2);
            String hashJoinRequest = genHash(joinRequestNodeId);
            joinedPorts.add(joinRequestNodeId);
            sortedPortHashValues.add(hashJoinRequest);
            portHash.put(hashJoinRequest, joinRequestNodeId);
            Collections.sort(sortedPortHashValues, new HashComparator());

            for (int index = 0; index < joinedPorts.size(); index++) {
                String node = portHash.get(sortedPortHashValues.get((index)));
                String succReply = portHash.get(sortedPortHashValues.get((index + 1) % sortedPortHashValues.size()));
                String predReply;
                if (index == 0)
                    predReply = portHash.get(sortedPortHashValues.get(sortedPortHashValues.size() - 1));
                else predReply = portHash.get(sortedPortHashValues.get(index - 1));
                String prePortReply = String.valueOf(Integer.parseInt(predReply) * 2);
                String sucPortReply = String.valueOf(Integer.parseInt(succReply) * 2);

                // Append the pred/suc of each AVD and send the message as a join acceptance to
                // the particular AVD

                String replyToJoin = "JA:" + prePortReply + ":" + sucPortReply;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replyToJoin, String.valueOf(Integer.parseInt(node) * 2));

            }
        }

        public void processJoinAcceptance(String pred, String suc) {
            // Update the predecessor, and successor after a join acceptance from emulator 5554
            updatePredSuc(pred, suc);
        }

        public void processInsertRequest(String key, String value) {
            // Process an insert request, query own Content Provider, else relay request to successor
            // Reference : PA2A
            ContentValues keyValueToInsert = new ContentValues();
            keyValueToInsert.put("key", key);
            keyValueToInsert.put("value", value);
            insert(mUri, keyValueToInsert);
        }

        public void processInsertSuccess(String message, String key) {
            // Process insertion success
            // Store in remote keys, and relay the insertion success
            // along the ring
            if (!listOfLocalInsertedKeys.contains(key) && !listOfRemoteKeys.contains(key)) {
                listOfRemoteKeys.add(key);
            }
            if (!insertionSuccessRelayed.contains(message)) {
                insertionSuccessRelayed.add(message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, sucId);
            }
        }

        public void processQueryRequest(String message, String queryKey, String queryRequestFrom) {
            // Process query request
            // if key locally present, return result, else relay the quest to successor
            if (listOfLocalInsertedKeys.contains(queryKey)) {
                String queryValue = retrieveFromFile(queryKey);
                String queryResult = "QA:" + queryKey + ":" + queryValue;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResult, queryRequestFrom);
            } else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, sucId);
            }
        }

        public void processQueryAnswer(String resultKey, String resultValue) {
            // Process query answer
            // Release lock on blocking while call in query
            if (queryResult.get(resultKey) == null) {
                queryResult.put(resultKey, resultValue);
                String queryResult = "QA:" + resultKey + ":" + resultValue;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResult, predId);
            }
        }

        public void processDelete(String message, String keyToDelete) {
            // Process delete
            // If key present locally, delete from local o/w from remote keys
            // and notify others to delete the key
            if (listOfLocalInsertedKeys.contains(keyToDelete))
                listOfLocalInsertedKeys.remove(keyToDelete);
            if (listOfRemoteKeys.contains(keyToDelete)) listOfRemoteKeys.remove(keyToDelete);
            if (!deletionSuccessRelayed.contains(message)) {
                deletionSuccessRelayed.add(message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, sucId);
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            /*
             * TODO: Fill in your client code that sends out a message.
             *
             * The following code writes string data to the server and reads the acknowledgement from the server
             * Reference :
             * 1. https://stackoverflow.com/questions/17440795/send-a-string-instead-of-byte-through-socket-in-java
             * 2. PA1 Code
             */
            try {
                String port = msgs[1];
                String msgToSend = msgs[0];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                Log.d("Sending to" + port, msgToSend);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(msgToSend);
                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                String receivedFromServer = dataInputStream.readUTF();
                socket.close();
                Log.d("Sent to" + port, msgToSend);


            } catch (UnknownHostException e) {
                Log.e(myPort, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(myPort, "ClientTask socket IOException" + e);
            }
            return null;
        }
    }

    private class HashComparator implements Comparator<String> {
        // The following code is used f0r lexicographical comparison of hash values
        @Override
        public int compare(String lhs, String rhs) {
            return lhs.compareTo(rhs);
        }

        @Override
        public boolean equals(Object object) {
            return false;
        }
    }
}
