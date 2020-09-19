(ns kbs.query
  (:require  [taoensso.timbre :as timbre]
             [ring.middleware.json :as middleware]
             [clojure.data.json :as json]
             [korma.db :refer :all]
             [kbs.config :as config]
             [clj-http.client :as httpclient]
             [clj-time.coerce :as tc]
             [clj-http.client :as client]
             [clojure.string :as string]
             [korma.core :refer :all]
             [clj-time.core :as t]
             [clj-time.format :as f]
             [clj-time.coerce :as tc]
             )
  (:import java.util.concurrent.Executors)
  (:import java.util.concurrent.TimeUnit)
  )

(declare billing)

(config/initialize-db)

(def db-connection-kbs
  {:classname "org.postgresql.Driver"
   :subprotocol config/db-subprotocol
   :user config/db-user
   :password config/db-pass
   :subname config/db-subname
   })


(def alphanumeric "ABCDEFGHJKLMNPQRSTUVWXYZ1234567890")
(defn get-random-id [length]
  (loop [acc []]
    (if (= (count acc) length) (apply str acc)
                               (recur (conj acc (rand-nth alphanumeric))))))

(def alphanumerickey "abcdefghijklm1234567890nopqrstuvwxyz")
(defn get-random-key [length]
  (loop [acc []]
    (if (= (count acc) length) (apply str acc)
                               (recur (conj acc (rand-nth alphanumerickey))))))

(def numerickey "1234567890")
(defn get-random-numeric-key [length]
  (loop [acc []]
    (if (= (count acc) length) (apply str acc)
                               (recur (conj acc (rand-nth numerickey))))))


(defn numeric? [s]
  (if-let [s (seq s)]
    (let [s (if (= (first s) \-) (next s) s)
          s (drop-while #(Character/isDigit %) s)
          s (if (= (first s) \.) (next s) s)
          s (drop-while #(Character/isDigit %) s)]
      (empty? s))))


(defn create-account [body]
  (let [acrefno (str (get-random-id 8))]
    (with-db db-connection-kbs (exec-raw (format "INSERT INTO accounts(refno, acdetails)
                                            VALUES ('%s','%s');"
                                                 acrefno  (clojure.string/replace (json/write-str body) #"'" "''") )))

    (with-db db-connection-kbs (exec-raw (format "SELECT refno, acdetails::text accountdetails, dateadded::text dateadded
                                                FROM accounts
                                                WHERE refno ='%s';"
                                                 acrefno) :results)
             )
    )

  )

(defn get-accounts [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT refno, acdetails::text accountdetails, dateadded::text dateadded
                                                FROM accounts
                                                ORDER BY (acdetails->>'ac_priority')::int ASC;") :results)
           )
  )

(defn get-account [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT refno, acdetails::text accountdetails, dateadded::text dateadded
                                                FROM accounts
                                                ORDER BY (acdetails->>'ac_priority')::int ASC;") :results)
           )
  )

(defn get-account-bal [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT COALESCE(SUM(last_transaction_change),0)::int trxbal
                                                  FROM accounts_fill
                                                  WHERE fleet_no='%s' AND accountref='%s' AND dateupdated::date = now()::date;"
                                               (get-in body ["fleet_no"]) (get-in body ["account"])) :results)
           )
  )



(defn create-bus [body]
  (let [brefno (str (get-random-id 8))]
    (with-db db-connection-kbs (exec-raw (format "INSERT INTO buses(refno, busdetails)
                                            VALUES ('%s','%s');"
                                                 brefno  (clojure.string/replace (json/write-str body) #"'" "''") )))

    (with-db db-connection-kbs (exec-raw (format "SELECT refno, busdetails::text busdetails, dateadded::text dateadded
                                                FROM buses
                                                WHERE refno ='%s';"
                                                 brefno) :results)
             )
    )

  )


(defn get-buses [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT b.refno, b.busdetails::text busdetails, a.acdetails->>'ac_name' accountname
                                                FROM buses b
                                                INNER JOIN accounts a
                                                ON a.refno = b.busdetails->'accounts'->0->>'account_no';") :results)
           )
  )

(defn get-bus-bus-reg-no [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT refno, busdetails::text busdetails, dateadded::text dateadded
                                                FROM buses
                                                WHERE busdetails->>'bus_reg_no'::text ='%s';"
                                               (get-in body ["bus_reg_no"])) :results)
           )
  )

(defn get-bus-bus-fleet-no [body]

  (with-db db-connection-kbs (exec-raw (format "SELECT refno, busdetails::text busdetails, dateadded::text dateadded
                                                FROM buses
                                                WHERE busdetails->>'fleet_no'::text ='%s';"
                                               (get-in body ["fleet_no"])) :results)
           )
  )


(defn create-offload [body]
  (let [offldref (str (get-random-id 8))
        abamt (if (number? (get-in body ["amount"])) (get-in body ["amount"]) (Integer/parseInt (get-in body ["amount"])))
        abamt (if (integer? abamt) abamt (Integer/parseInt (get-in body ["amount"])))
        ]

    (with-db db-connection-kbs (exec-raw (format "INSERT INTO transactions(refno, txdetails)
                                            VALUES ('%s','%s');"
                                                 offldref   (clojure.string/replace (json/write-str body) #"'" "''") )))


    (let [accounts (with-db db-connection-kbs (exec-raw (format "SELECT busdetails->>'accounts'::text accountnos, jsonb_array_length(busdetails->'accounts') accts
                                                  FROM buses b
                                                  WHERE busdetails->>'fleet_no'::text ='%s' AND busdetails->>'accounts' IS NOT NULL;"
                                                                (get-in body ["fleet_no"])) :results)
                            )]
      ;bus has these accounts
      (doseq [a (clojure.walk/keywordize-keys (json/read-str (get-in (json/read-str (json/write-str (first accounts) )) ["accountnos"])))
              ;transactionbal
              accountlimit (with-db db-connection-kbs (exec-raw (format "SELECT (acdetails->>'ac_amount')::int acamount
                                                FROM accounts
                                                WHERE refno ='%s';"
                                                                        (:account_no a)) :results)
                                    )
              checkentrytoday (with-db db-connection-kbs (exec-raw (format "SELECT count(refno) accountexiststoday
                                                FROM accounts_fill
                                                WHERE dateupdated::date = now()::date AND accountref = '%s' AND fleet_no='%s' ;"
                                                                           (:account_no a) (get-in body ["fleet_no"]) ) :results)
                                       )

              ]
        (if (= (:accountexiststoday checkentrytoday) 1)        ;account exists
          ;check if limit is achieved
          (let [checkthistrxbal (with-db db-connection-kbs (exec-raw (format "SELECT COALESCE(SUM(last_transaction_change),0)::int trxbal
                                                FROM accounts_fill
                                                WHERE  fleet_no='%s' AND last_transaction_ref='%s';"
                                                                             (get-in body ["fleet_no"]) offldref) :results)
                                         )
                checkebals (with-db db-connection-kbs (exec-raw (format "SELECT current_bal::int current_bal
                                                FROM accounts_fill
                                                WHERE dateupdated::date = now()::date AND accountref = '%s' AND fleet_no='%s';"
                                                                        (:account_no a) (get-in body ["fleet_no"])) :results)
                                    )
                baldeficit (- (:acamount accountlimit) (:current_bal (first checkebals)))
                ;updateableamount

                ]
            ;(println (:acamount accountlimit))
            (if (< (:trxbal (first checkthistrxbal)) abamt)
              (if (> baldeficit 0)
                (if (> (- abamt (:trxbal (first checkthistrxbal))) baldeficit)
                  (with-db db-connection-kbs (exec-raw (format "UPDATE accounts_fill SET current_bal= current_bal + '%s', last_transaction_ref = '%s', last_transaction_change='%s'
                                            WHERE dateupdated::date = now()::date AND accountref = '%s' AND fleet_no='%s'"
                                                               baldeficit   offldref  (- (- abamt (:trxbal (first checkthistrxbal)))  baldeficit ) (:account_no a) (get-in body ["fleet_no"]) )))
                  (with-db db-connection-kbs (exec-raw (format "UPDATE accounts_fill SET current_bal= current_bal + '%s', last_transaction_ref = '%s', last_transaction_change='%s'
                                            WHERE dateupdated::date = now()::date AND accountref = '%s' AND fleet_no='%s'"
                                                               (- abamt (:trxbal (first checkthistrxbal)))   offldref  (- abamt (:trxbal (first checkthistrxbal)))  (:account_no a) (get-in body ["fleet_no"])  )))
                  )
                )
              )
            )
          (let [
                checkthistrxbal (with-db db-connection-kbs (exec-raw (format "SELECT COALESCE(SUM(last_transaction_change),0)::int trxbal
                                                FROM accounts_fill
                                                WHERE fleet_no='%s' AND last_transaction_ref='%s';"
                                                                             (get-in body ["fleet_no"]) offldref) :results)
                                         )
                checkebals 0
                   baldeficit (:acamount accountlimit)

                   ]

            (if (< (:trxbal (first checkthistrxbal)) abamt)
              (if (> abamt baldeficit)
                (with-db db-connection-kbs (exec-raw (format "INSERT INTO accounts_fill(refno, accountref, accountlimit, fleet_no, current_bal, last_transaction_ref, last_transaction_change)
                                            VALUES ('%s','%s','%s','%s','%s','%s','%s');"
                                                             (str (get-random-id 8))  (:account_no a) (:acamount accountlimit) (get-in body ["fleet_no"]) baldeficit offldref (- abamt baldeficit))))
                (with-db db-connection-kbs (exec-raw (format "INSERT INTO accounts_fill(refno, accountref, accountlimit, fleet_no, current_bal, last_transaction_ref, last_transaction_change)
                                            VALUES ('%s','%s','%s','%s','%s','%s','%s');"
                                                             (str (get-random-id 8))  (:account_no a) (:acamount accountlimit) (get-in body ["fleet_no"]) abamt  offldref abamt)))
                ))

               )
          )
        )
      )
    ;;then loop the accounts while checking today's takings for limits


    (with-db db-connection-kbs (exec-raw (format "SELECT refno, txdetails::text txdetails, dateadded::text dateadded
                                                FROM transactions
                                                WHERE refno ='%s';"
                                                 offldref) :results)
             )

    )

  )

(def bamt (atom 0))
(defn create-offload00 [body]
  (let [offldref (str (get-random-id 8))
        abamt (if (number? (get-in body ["amount"])) (get-in body ["amount"]) (Integer/parseInt (get-in body ["amount"])))
        ]

    ;(timbre/info "=====================> " (number? abamt))

    ; (swap! abamt (partial + (get-in body ["amount"])))
    (with-db db-connection-kbs (exec-raw (format "INSERT INTO transactions(refno, txdetails)
                                            VALUES ('%s','%s');"
                                                 offldref   (clojure.string/replace (json/write-str body) #"'" "''") )))

    #_(json/write-str
        {
         :amount   (crypt/encrypt "pesa.express.greatest.of.all.time")
         :tx_ref    username
         :ac_ref      (if (= otp "") nil otp)
         :thetime (str (tc/to-timestamp (java.util.Date.)))
         }
        )
    ;create an account entry so that we can view a summary of the offloads, by checking the accounts that are subscribed by the bus
    #_(with-db db-connection-kbs (exec-raw (format "SELECT refno, acquotadetails::text acquotadetails, dateadded::text dateadded
                                                FROM account_quotas
                                                WHERE dateadded::date = now()::date
                                                AND acquotadetails->>'fleet_no'::text ='%s';"
                                                 (get-in body ["fleet_no"])) :results)
             )


    (let [accounts (with-db db-connection-kbs (exec-raw (format "SELECT busdetails->>'accounts' accountnos, jsonb_array_length(busdetails->'accounts') accts
                                                  FROM buses b
                                                  WHERE busdetails->>'fleet_no'::text ='%s';"
                                                                (get-in body ["fleet_no"])) :results)
                            )]
      ;bus has these accounts
      (doseq [a (clojure.walk/keywordize-keys (json/read-str (get-in (json/read-str (json/write-str (first accounts) )) ["accountnos"])))
              ;transactionbal
              accountlimit (with-db db-connection-kbs (exec-raw (format "SELECT (acdetails->>'ac_amount')::real acamount
                                                FROM accounts
                                                WHERE refno ='%s';"
                                                                        (:account_no a)) :results)
                                    )

              todaysintakeamount (with-db db-connection-kbs (exec-raw (format "SELECT COALESCE(SUM((acquotadetails->>'amount')::float),0) intakeamount
                                                FROM account_quotas
                                                WHERE dateadded::date = now()::date
                                                AND acquotadetails->>'fleet_no'::text ='%s'
                                                AND acquotadetails->>'ac_ref'::text ='%s';"
                                                                        (get-in body ["fleet_no"])  (:account_no a)) :results)
                                    )
              pendingtxbal (with-db db-connection-kbs (exec-raw (format "SELECT coalesce(SUM((acquotadetails->>'amount')::real),0) pbal
                                                FROM account_quotas
                                                WHERE  acquotadetails->>'tx_ref'::text ='%s';"
                                                                        offldref) :results)
                                          )

              ]
        ;(println "1-->" (:pbal pendingtxbal))
        ; (println "2-->" abamt)

        ; (println "3-->" (- abamt (:pbal pendingtxbal) ) )

        ; (println "4-->" (- abamt (- abamt (:pbal pendingtxbal) )) )
        ; (println "5-->" (:acamount accountlimit) )
        ; (println "6-->" (:intakeamount todaysintakeamount)  )
        ; (println "--" (:acamount accountlimit))
        ;(println "**" (float (:intakeamount todaysintakeamount)))
        ;(println "neg**" (- (:acamount accountlimit) (:intakeamount todaysintakeamount)))
        ;(println "neg**" (- abamt (- (:acamount accountlimit) (:intakeamount todaysintakeamount))))
        ;(println "55" (>  abamt (:acamount accountlimit)) )
        ;(println "55" (>  abamt (- (:acamount accountlimit) (:intakeamount todaysintakeamount))) )


        (if (or (= (- abamt (:pbal pendingtxbal) ) 0) (= (- abamt (:pbal pendingtxbal) ) 0.0))
          ; (println "cant top up. out of cash")
          (if (not= (:acamount accountlimit) (:intakeamount todaysintakeamount) )
            (if (<  (- abamt (:pbal pendingtxbal)) (:acamount accountlimit))
              (do
                (with-db db-connection-kbs (exec-raw (format "INSERT INTO account_quotas(refno, acquotadetails)
                                            VALUES ('%s','%s');"
                                                             (str (get-random-id 8))   (json/write-str
                                                                                         {
                                                                                          :amount   (- (:acamount accountlimit) (+ (:pbal pendingtxbal) (:intakeamount todaysintakeamount)))
                                                                                          :tx_ref    offldref
                                                                                          :ac_ref      (:account_no a)
                                                                                          :fleet_no (get-in body ["fleet_no"])
                                                                                          :thetime (str (tc/to-timestamp (java.util.Date.)))
                                                                                          }
                                                                                         ) )))
                )
              (with-db db-connection-kbs (exec-raw (format "INSERT INTO account_quotas(refno, acquotadetails)
                                            VALUES ('%s','%s');"
                                                           (str (get-random-id 8))   (json/write-str
                                                                                       {
                                                                                        :amount   (- abamt (+ (:pbal pendingtxbal) (:intakeamount todaysintakeamount)) )
                                                                                        :tx_ref    offldref
                                                                                        :ac_ref      (:account_no a)
                                                                                        :fleet_no (get-in body ["fleet_no"])
                                                                                        :thetime (str (tc/to-timestamp (java.util.Date.)))
                                                                                        }
                                                                                       ) )))
              )
            ;(println "okay account")
            )
          )
        )
      )
    ;;then loop the accounts while checking today's takings for limits


    (with-db db-connection-kbs (exec-raw (format "SELECT refno, txdetails::text txdetails, dateadded::text dateadded
                                                FROM transactions
                                                WHERE refno ='%s';"
                                                 offldref) :results)
             )

    )

  )

(defn get-offloads [body]

  (with-db db-connection-kbs (exec-raw (format "SELECT af.refno,af.accountref,af.fleet_no, af.accountlimit,af.current_bal,a.acdetails->>'ac_name'::text accountname,b.busdetails->>'bus_capacity'::text bus_capacity
                                                FROM accounts_fill af
                                                INNER JOIN accounts a
                                                ON af.accountref = a.refno
                                                INNER JOIN buses b
                                                ON b.busdetails->>'fleet_no'::text = af.fleet_no
                                                --WHERE af.dateupdated::date = now()::date
                                                ;"
                                               (get-in body ["datefrom"]) (get-in body ["dateto"])) :results)
           )
  )

(defn get-offloads-full [body]

  (with-db db-connection-kbs (exec-raw (format "SELECT b.busdetails->>'fleet_no'::text fleetno, txdetails::text txdetails, busdetails::text busdetails, coalesce(SUM((ac.acquotadetails->>'amount')::real),0) pbal
                                                  FROM buses b
                                                  INNER JOIN transactions t
                                                  ON t.txdetails->>'fleet_no'::text = b.busdetails->>'fleet_no'::text
                                                  INNER JOIN account_quotas ac
                                                  ON ac.acquotadetails->>'fleet_no'::text = b.busdetails->>'fleet_no'::text
                                                  WHERE t.dateadded::date = now()::date
                                                  GROUP BY 1,2,3;") :results)
           )
  )

(defn get-offload-offload-by [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT refno, txdetails::text txdetails, dateadded::text dateadded
                                                FROM transactions
                                                WHERE txdetails->>'offloadby'::text ='%s';"
                                               (get-in body ["offloadby"])) :results)
           )
  )

(defn get-offload-offload-from [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT refno, txdetails::text txdetails, dateadded::text dateadded
                                                FROM transactions
                                                WHERE txdetails->>'offloadfrom'::text ='%s';"
                                               (get-in body ["offloadfrom"])) :results)
           )
  )

(defn get-offload-fleet-no [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT refno, txdetails::text txdetails, dateadded::text dateadded
                                                FROM transactions
                                                WHERE txdetails->>'fleet_no'::text ='%s';"
                                               (get-in body ["fleet_no"])) :results)
           )
  )

(defn get-offload-offload-by-summary [body]
  (with-db db-connection-kbs (exec-raw (format "SELECT m.refno, m.productname, onsale onsale, s.currency::text currency, category productcategory, c.categoryname, c.categoryavatar, LOWER(replace(replace(replace(replace(categoryname, ' &amp; ', '-'), ', ', '-'),' ','-') ,':','')) catname, tags::text tags,
                                            replace(producttype, ',', '') producttype, sizes::text productsize, packaging, sellingpricerounded::integer sellingprice, newsellingprice::integer newsellingprice, TO_CHAR(sellingpricerounded::integer, '999,999') sellingpricedisplay,s.shopname, m.isactive activestatus,
                                            case when m.isactive=0 then 'Inactive' when m.isactive=1 then 'Active' when m.isactive=2 then 'Suspended' when m.isactive=3 then 'Deactivated' else 'Status Unknown' end as activereq, url, pics::text pics, detaileddescription ddescription,
                                            coalesce((sizes-> jsonb_array_length(sizes) -1  ->>'stock')::integer,0) as currentstock, ROW_NUMBER () OVER (ORDER BY m.id DESC) listedas
                                            FROM market m
                                            INNER JOIN category c
                                            ON c.refno = m.category
                                            INNER JOIN shop s
                                            ON s.refno = m.shopref
                                            where coalesce((sizes-> jsonb_array_length(sizes) -1  ->>'stock')::integer,0) > 0
                                            --AND m.id>144
                                            AND m.isactive = 0
                                            AND s.isactive = 1
                                            ORDER BY 19 asc;" ) :results))
  )