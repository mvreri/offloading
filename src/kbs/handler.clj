(ns kbs.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.json :as middleware]
            [ring.middleware.cors :refer [wrap-cors]]
            [kbs.query :as query]
            [taoensso.timbre :as timbre]
            [clojure.data.json :as json]
            [kbs.config :as config]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [clojure.string :as string]))

(defroutes app-routes
  (GET "/" [] "Hello World")
           (POST "/api/v1/bus/create" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 201
                                                  :title (str "Bus Creation" )
                                                  :detail (query/create-bus body)
                                                  }
                                           })
                             )

               )
             )
           (POST "/api/v1/buses/view" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 200
                                                  :title (str "Buses" )
                                                  :detail (query/get-buses body)
                                                  }
                                           })
                             )

               )
             )
  (POST "/api/v1/bus/view" request
    (let [body (:body request)
          keybody (clojure.walk/keywordize-keys body)
          ]
      (if (contains? keybody :bus_reg_no)
        (with-out-str (json/pprint {:data {
                                           :status 200
                                           :title (str "Bus  " (get-in body ["bus_reg_no"]))
                                           :detail (query/get-bus-bus-reg-no body)
                                           }
                                    })
                      )
        (if (contains? keybody :fleet_no)
          (with-out-str (json/pprint {:data {
                                             :status 200
                                             :title (str "Bus  " (get-in body ["fleet_no"]))
                                             :detail (query/get-bus-bus-fleet-no body)
                                             }
                                      })
                        )
          (with-out-str (json/pprint {:errors {
                                               :status 422
                                               :title (str "Incorrect Params")
                                               :detail (str "Incorrect Params Specified")
                                               :message (str "Incorrect Params Specified")
                                               }
                                      })
                        )
          )

        )


      )
    )

           (POST "/api/v1/bus/offload" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 201
                                                  :title (str "Bus Offload Creation" )
                                                  :detail (query/create-offload body)
                                                  }
                                           })
                             )

               )
             )

           (POST "/api/v1/bus/offloads/view" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 200
                                                  :title (str "Bus Offloads" )
                                                  :detail (query/get-offloads body)
                                                  }
                                           })
                             )

               )
             )

           (POST "/api/v1/bus/offload/view" request
             (let [body (:body request)
                   keybody (clojure.walk/keywordize-keys body)
                   ]
               (if (contains? keybody :offloadby)
                 (with-out-str (json/pprint {:data {
                                                    :status 200
                                                    :title (str "Offload By  " (get-in body ["offloadby"]))
                                                    :detail (query/get-offload-offload-by body)
                                                    }
                                             })
                               )
                 (if (contains? keybody :offloadfrom)
                   (with-out-str (json/pprint {:data {
                                                      :status 200
                                                      :title (str "Offload From  " (get-in body ["offloadfrom"]))
                                                      :detail (query/get-offload-offload-from body)
                                                      }
                                               })
                                 )
                   (if (contains? keybody :fleet_no)
                     (with-out-str (json/pprint {:data {
                                                        :status 200
                                                        :title (str "Offload By  " (get-in body ["fleet_no"]))
                                                        :detail (query/get-offload-fleet-no body)
                                                        }
                                                 })
                                   )
                     (with-out-str (json/pprint {:errors {
                                                          :status 422
                                                          :title (str "Incorrect Params")
                                                          :detail (str "Incorrect Params Specified")
                                                          :message (str "Incorrect Params Specified")
                                                          }
                                                 })
                                   )
                     )

                   )

                 )


               )
             )

           (POST "/api/v1/account/create" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 201
                                                  :title (str "Account Creation" )
                                                  :detail (query/create-account body)
                                                  }
                                           })
                             )

               )
             )

           (POST "/api/v1/accounts/view" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 200
                                                  :title (str "Accounts" )
                                                  :detail (query/get-accounts body)
                                                  }
                                           })
                             )

               )
             )

           (POST "/api/v1/account/view" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 200
                                                  :title (str "Accounts" )
                                                  :detail (query/get-account body)
                                                  }
                                           })
                             )

               )
             )

           (POST "/api/v1/account/balance/view" request
             (let [body (:body request)]
               (with-out-str (json/pprint {:data {
                                                  :status 200
                                                  :title (str "Account Balance" )
                                                  :detail (query/get-account-bal body)
                                                  }
                                           })
                             )

               )
             )
  (route/not-found "Not Found"))

(def app
  (-> app-routes
      (middleware/wrap-json-body)
      (middleware/wrap-json-response)
      (wrap-defaults app-routes)
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:post :get]
                 ;:access-control-allow-header
                 #_["Access-Control-Allow-Origin" "*"
                  "Origin" "X-Requested-With"
                  "Content-Type" "Accept"]
                 )))
