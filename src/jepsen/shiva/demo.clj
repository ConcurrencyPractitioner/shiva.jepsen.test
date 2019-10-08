(ns jepsen.shiva.demo
    (:gen-class)
    (:require [clojure.java.io :as io]
	      [clojure.core.reducers :as r]
	      [clojure.tools.logging :refer :all]
	      [knossos.model :as model]
	      [knossos.op :as op]
	      [jepsen.db :as db]
              [jepsen.cli :as cli]
              [jepsen.checker :as checker]
              [jepsen.client :as client]
              [jepsen.control :as control]
              [jepsen.generator :as gen]
              [jepsen.nemesis :as nemesis]
              [jepsen.tests :as tests]
              [jepsen.os.centos :as centos]
              [jepsen.control.util :as control-util]
              [jepsen.client :as client])
    (:import  (io.transwarp.shiva.client Session$FlushMode)
	      (io.transwarp.shiva.kv Get)
              (io.transwarp.shiva.kv KvMeta) 
	      (io.transwarp.shiva.kv SchemaBuilder)
	      (io.transwarp.shiva.client ShivaClient) 
	      (io.transwarp.shiva.client TableCreator) 
	      (io.transwarp.shiva.common ErrorCode)
	      (io.transwarp.shiva.utils Bytes)
	      (io.transwarp.shiva.kv Table)
	      (io.transwarp.shiva.kv KvClient)
	      (knossos.model Model)
	      (java.util HashMap)
	      (java.util ArrayList)
	      (java.lang Integer)
	      (java.lang System)
	      (io.transwarp.shiva.common Options)
	    ))

(def threshold 10000)
(def user "shiva")
(def base-dir "/opt/shiva")
(def table_name "new_hash_table")
(def temp_table_name "temp_table_name_")
(def cf_name "column family")
(def keyMax 10)
(def halfKeyMax 5)
(def valueMax 10000)
(def maxTimeout 15000)
(def joinBufferTime 20)
(def nodeVector ["192.168.1.163:9000" "192.168.1.164:9000" "192.168.1.167:9000"])

(defn aggregateMapList [opMapList]
    (let [result (HashMap.)
	  listIterator (.iterator opMapList)]
	(while (.hasNext listIterator)
	    (let [opMap (-> listIterator .next)
		  opMapIterator (-> opMap .keySet .iterator)]
	    	(while (.hasNext opMapIterator)
		    (let [key (.next opMapIterator)]
		        (-> result (.put key (-> opMap (.get key))))))))
	result
    )
)

(defn LatencyChecker
  []
  (reify checker/Checker
    (check [checker test history opts]
      (let [ops (->> history
                       (r/filter op/ok?)
                       (into []))
	    iterator (.iterator ops)
	    invalidOps (ArrayList.)]
        (while (.hasNext iterator)
	    (let [nextOp (.next iterator)]
	        (if (> (:latency nextOp) threshold)
	            (-> invalidOps (.add nextOp))
	        )))
	
	{:valid? (= (.size invalidOps) 0)
	 :invalidOperations invalidOps}))))

(defn db []
 ; [tarball-url]
  (reify db/DB
         (setup! [_ test node]
		(info "Do nothing - setup"))
;                ((install! node tarball-url)
;		  (configureAndStart! node test)))
         (teardown! [_ test node]
		(info "Do nothing - teardown"))))
;                    (if (= node (getMasterNode node))
;			(nukeMaster! node)
;			(nukeTServer! node)))))

(defn fatal-nemesis
  "Wraps a nemesis. Used in attempt to delete nodes permanently"
  [nem]
  (reify nemesis/Nemesis
    (setup! [this test]
      (nemesis/setup! nem test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (try (nemesis/invoke! nem test op)
	            (finally (control/exec :curl :-XDELETE (str "http://192.168.1.160:4567/server/" (nth nodeVector (rand-int 2)))))) 

        :stop (try (nemesis/invoke! nem test op)
                   (finally
                     (info "Do nothing for stop")))
      ))

    (teardown! [this test]
      (nemesis/teardown! nem test))))

(defn ^Table constructKVTable [^ShivaClient client ^KvClient kvClient]
  (let [tableCreator (.newTableCreator client)
        schemaBuilder (SchemaBuilder.)
        kvMeta (KvMeta. table_name)]
    (try
     	(if (-> client (.newTableExister table_name) .isExist)
		(-> client (.newTableDeleter table_name) .delete)
    	)
	(catch Exception exc) ;lazy way to do things, but ensures that previous table which was created does not exist
    )
    (-> schemaBuilder
        (.addColumn cf_name)) 
    (-> kvMeta (.setSchema (.build schemaBuilder)))
    (-> kvMeta (.setNumBuckets 3))
    (-> kvMeta (.setCapacityUnit 1))
    (-> tableCreator (.addMeta kvMeta))
    (.create tableCreator)
    (-> kvClient (.openTable table_name))))

(defn ^Table constructTempKVTable [^ShivaClient client ^KvClient kvClient]
  (let [tableCreator (.newTableCreator client)
        schemaBuilder (SchemaBuilder.)
	new_table_name (str temp_table_name (rand-int Integer/MAX_VALUE))
        kvMeta (KvMeta. new_table_name)]
    (try
        (-> client (.newTableDeleter new_table_name) .delete)
	(catch Exception exc) ;lazy way to do things, but ensures that previous table which was created does not exist
    )
    (-> schemaBuilder
        (.addColumn cf_name)) 
    (-> kvMeta (.setSchema (.build schemaBuilder)))
    (-> kvMeta (.setNumBuckets 1000))
    (-> kvMeta (.setCapacityUnit 1))
    (-> tableCreator (.addMeta kvMeta))
    (.create tableCreator)
    (-> kvClient (.openTable new_table_name))))

(defn ^java.util.HashMap scanBatch [scanner]
  (try
    (let [batch (-> scanner (.nextBatch) (.join))
          iterator (.iterator batch)
	  result (HashMap.)]
      (while (.hasNext iterator)
             (let [resultPtr (.next iterator)]
               (if not(= (.getCode resultPtr) (ErrorCode/OK))
		 ((info "returning nil for scan")
                 nil))

	       (-> result (.put (-> resultPtr .getKey (.getInt 0))
				(-> resultPtr .getValue (.getInt 0))))))
      result)
    (catch Exception e
      (info "Exception caught")
      nil)))

(defn getAndAwaitOp [kvTable getOp]
  (-> getOp (.SetConsistentRead true))
  (let [deferredResult (-> kvTable (.get getOp))]
    (-> deferredResult (.join (+ ( / maxTimeout 2) joinBufferTime)))
))

(defn getValueFromSlice [slice] 
  (try
     (-> slice (.getInt 0))
     (catch Exception exc
	-1)
  )
)

(defn ^java.util.HashMap getAndAwaitBatch [getBatchOp kvTable k1 k2]
   (-> getBatchOp (.setConsistentRead true))
   (-> getBatchOp (.add cf_name (Bytes/fromInt k1)))
   (-> getBatchOp (.add cf_name (Bytes/fromInt k2)))
   (let [deferredResults (-> kvTable (.batchGet getBatchOp))
	 resultMap (HashMap.)]
      (let [iterator (.iterator deferredResults)]
	(while (.hasNext iterator)
	   (let [res (-> iterator .next (.join (+ (* joinBufferTime 10) (* maxTimeout 2))))
		 rangeVector (range 0 (.numRows res))
		 rangeIterator (.iterator rangeVector)]
		(while (.hasNext rangeIterator)
		    (let [i (.next rangeIterator)
			  resultPtr (-> res (.result i))]
		        (-> resultMap (.put (-> resultPtr .getKey (.getInt 0))
                                            (getValueFromSlice (.getValue resultPtr))))
		    )))))
	resultMap))

(defn getValue [value]
    (try
	(Bytes/getInt value)
	(catch Exception exc
           -1)
    )
)


(defn p [_ _] {:type :invoke, :f :put, :value [(rand-int keyMax) (rand-int valueMax)]})
(defn handlePut[kvTable session op]
   (let [currTime (System/currentTimeMillis) 
	 [key value] (:value op)
         putOp (-> kvTable (.newPut cf_name)) ]
       (-> putOp (.setKeyValue (Bytes/fromInt key) (Bytes/fromInt  value)))
       (-> session (.apply putOp))
       (assoc op :type :ok :output [key value] :latency (- (System/currentTimeMillis) currTime)))
)

(defn d [_ _] {:type :invoke, :f :delete, :value (rand-int halfKeyMax)})
(defn handleDelete[kvTable session op]
    (let [currTime (System/currentTimeMillis)
	  deleteOp (-> kvTable (.newDelete cf_name))]
         (-> deleteOp (.setKey (Bytes/fromInt (:value op))))
                   (-> session (.apply deleteOp))
                            (assoc op :type :ok :latency (- (System/currentTimeMillis) currTime)))
)

(defn s [_ _] {:type :invoke, :f :scan, :value nil})
(defn handleScan[kvTable op]
    (let [currTime (System/currentTimeMillis)
	  scanOp (-> kvTable (.newScan cf_name))]
	   (info "Scan is being called")
           (-> scanOp (.setConsistentRead true))
           (let [scanner (-> kvTable (.openScanner scanOp))
                 batchInformation (ArrayList.)]
              (while (not(.isFinished scanner))
                 (let [scan-res (scanBatch scanner)]
                      (info "scan-res result was: " scan-res)
                           (if (nil? scan-res) (assoc op :type :fail))
		       (-> batchInformation (.add scan-res))
  		 ))
	(.close scanner)
	(assoc op :type :ok :value (aggregateMapList batchInformation) :latency (- (System/currentTimeMillis) currTime))))
)

(defn g [_ _] {:type :invoke, :f :get, :value (rand-int keyMax)})
(defn handleGet[kvTable op]
     (let [currTime (System/currentTimeMillis)
	   getOp (Get. cf_name (Bytes/fromInt (:value op)))]
         (try
             (let [result (getAndAwaitOp kvTable getOp)]
		(assoc op :type :ok :value [(Bytes/getInt (.key result)) (getValue (.value result))] 
			  :latency (- (System/currentTimeMillis) currTime)))
             (catch Exception exc
                (info "caught exception from get op" exc)
                (assoc op :type :fail :latency (- (System/currentTimeMillis) currTime)))))
)

(defn bg [_ _] {:type :invoke, :f :batchGet, :value [(rand-int keyMax) (rand-int keyMax)]})
(defn handleBatchGet[kvTable op]
    (let [ currTime (System/currentTimeMillis)
	   getBatchOp (.newGets kvTable) 
	   [key1 key2] (:value op)]
	(try
	    (info "values are " (:value op))
	    (let [resultsReturned (getAndAwaitBatch getBatchOp kvTable key1 key2)]
		(info "Output for batch is " resultsReturned)
		(info "Size of batch output is " (count resultsReturned))
		(assoc op :type :ok :value resultsReturned :latency (- (System/currentTimeMillis) currTime))
	    )
	    (catch Exception exc
		(info "Exception caught from batch get: " exc)
		(assoc op :type :fail :latency (- (System/currentTimeMillis) currTime))
))))

(defn extractOriginalBatchGetValues [output]
    (info "map to be extracted has value " output)
    (let [res (ArrayList.)
	  iterator (-> output .keySet .iterator)]
	(while (.hasNext iterator) (-> res (.add (.next iterator))))
	(try 
	   [(-> res (.get 0)) (-> res (.get 1))]
	   (catch Exception exc
	    [(-> res (.get 0)) (-> res (.get 0))] ))
    )
)    

(defn ct [_ _] {:type :invoke, :f :createTable, :value nil})
(defn createTempTable[op]
    (try
        (let [currTime (System/currentTimeMillis)
	      conn (ShivaClient/getInstance)
              options (Options.)]
            (set! (.masterGroup options) "192.168.1.160:9630,192.168.1.161:9630,192.168.1.163:9630")
            (set! (.rpcTimeoutMs options) 1000)
	    (set! (.writeRpcTimeoutMs options) 1000)
            (-> conn (.start options))
	    (let [kvConn (.newKvClient conn)
		  newTable (constructTempKVTable conn kvConn)]
                (assoc op :type :ok :value (.tableName newTable) :latency (- (System/currentTimeMillis) currTime))))
	(catch Exception exc 
	    (info "Exception caught while creating table: " exc)
	    (assoc op :type :fail :latency 100000)
        )))

(defn client "Method which returns client based on protocol"
  [conn kvTable session node]
  (reify client/Client
	 (setup! [_ _] )
         (open! [_ test node]
                   (let [conn (ShivaClient/getInstance)
                         options (Options.)]
                     (set! (.masterGroup options) "192.168.1.160:9630,192.168.1.161:9630,192.168.1.163:9630")
		     (set! (.rpcTimeoutMs options) 1000)
	 	     (set! (.writeRpcTimeoutMs options) 1000)
                     (-> conn (.start options))
		     (let [kvConn (.newKvClient conn)
			   session (.newSession kvConn)]
			 (-> session (.setFlushMode Session$FlushMode/AUTO_FLUSH_SYNC))
                         (client conn
                             (constructKVTable conn kvConn)
                             session node)))
		   )
         (invoke! [client test op]
                  (case (:f op)
                        :put (handlePut kvTable session op)
                        :delete (handleDelete kvTable session op)
			:scan (handleScan kvTable op)
                        :get (handleGet kvTable op)
			:batchGet (handleBatchGet kvTable op)
			:createTable 
			   (if (= (name node) "192.168.1.160")
				(createTempTable op)
				(assoc op :type :ok)
			   )))
         (teardown! [_ test]
                    ( ;(-> conn
                       ;  (.newTableDeleter table_name)
                       ;  (.delete))
		      ;(try (.close  conn) (catch Exception exc))

         ))))

(defn ^HashMap filterMap [hashMap]
    (let [resMap (HashMap.)
	  iterator (-> hashMap .keySet .iterator)]
	(info "map to be filtered: " hashMap)
	(while (.hasNext iterator)
	    (let [key (.next iterator)
		  value (-> hashMap (.get key))]
		(info "current key value is: " key " and " value)
		(if (> value -1) (-> resMap (.put key value)))
            )
	)
	(info "resMap value after filtering was: " resMap)
	resMap))

(defn putValue [table key value]
    (let [res (-> table (.remove key))]
        (-> table (.put key value)))
)

(defn getValueInMap [table key]
    (let [res (-> table (.get key))]	
	(if (nil? res) -1 res)
    )
)

(defn nextRes [correctMap mapToTest header op]
    (let [iterator (-> correctMap .keySet .iterator)]
        (while (.hasNext iterator)
            (let [key (.next iterator)
                 val1 (getValueInMap correctMap key)
                 val2 (-> mapToTest (.get key))]
                (if (not(= val1 val2))
		    (info header "values incorrect, should be: " correctMap " instead of " mapToTest " for " op)
                    false
                )
    )))
    (= (count correctMap) (count mapToTest))
)

(defn applyOp [table op]
    (case (:f op) 
	:put (let [[key value] (:value op)
		   res (putValue table key value)] (info "putting values " (:value op) " into store") true) 
        :delete (let [key (:value op)
		      res (putValue table key -1)] (info "deleting key " (:value op) "from store") true)
	:get (let [[key value] (:value op)
	           res (getValueInMap table key)] (info "value for get " op " should be " res) (= value res))
	:scan  (try 
		   (let [oldMap (:value op)]
		       (nextRes (filterMap table) oldMap "scan:" op) 
                   )
		   (catch Exception exc
		     false))
	:batchGet 
	(try 
	    (let [oldMap (:value op)
		  [key1 key2] (extractOriginalBatchGetValues oldMap)
		  resMap (HashMap.)]
		(-> resMap (.put key1 (getValueInMap table key1)))
		(-> resMap (.put key2 (getValueInMap table key2)))
		(nextRes resMap oldMap "batchGet:" op)
	    )
	    (catch Exception exc false)
)))

(defn getMap []
  (let [resMap (HashMap.)]
        (-> resMap (.put 0 -1))
        (-> resMap (.put 1 -1))
        (-> resMap (.put 2 -1))
        (-> resMap (.put 3 -1))
        (-> resMap (.put 4 -1))
        (-> resMap (.put 5 -1))
        (-> resMap (.put 6 -1))
        (-> resMap (.put 7 -1))
        (-> resMap (.put 8 -1))
        (-> resMap (.put 9 -1))
    resMap)
)

(defn HistoryChecker []
    (reify checker/Checker
        (check [checker test history opts]
            (let [ops (->> history
                           (r/filter op/ok?)
                           (into []))
                  iterator (.iterator ops)
                  hashMap (getMap)
                  invalidOps (ArrayList.)]
                (while (.hasNext iterator)
                    (let [nextOp (.next iterator)
                          valid (applyOp hashMap nextOp)]
                        (if (not valid) (-> invalidOps (.add nextOp)))
                    ))
              {:valid? (= (count invalidOps) 0)
               :invalidOperations invalidOps}))
))

(defn shiva-test [opts]
  ;(info "Creating test")
  ;(control/su (control/exec :echo "hello"))
  (merge tests/noop-test
         opts
         {:name "shiva"
          :client (client nil nil nil nil)
	  :db (db)
	  ;:nemesis (nemesis/partition-majorities-ring)
          :generator (->> (gen/mix [p d g bg]) ;ignore this comment, too lazy to delete :P disabled scan because of strange behavior
                          (gen/stagger 1)
			  (gen/nemesis (gen/seq (cycle [(gen/sleep 30)
				      	               {:type :info, :f :start}
				      	               (gen/sleep 30)
				      	               {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))
          :checker (checker/compose {:latency (LatencyChecker)
				     :perf (checker/perf)
				     :validation (HistoryChecker)
	  })}))

(defn -main [& args] "not much here yet"  ;for now, we just put this here, might break it up if file gets too large
  (cli/run! (merge (cli/single-test-cmd {:test-fn shiva-test})
                   (cli/serve-cmd))
            args))


