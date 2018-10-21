package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"fmt"
	"sync"
	"strconv"
	"bytes"
	"io/ioutil"
)

//item count message struct
type Message struct {
	ID        string   `json:"id"`
	Count 	  int   `json:"count"`
}

//status message for ping request
type Status struct{
	Message string `json:"message"`
}

//masternode endpoint
var masternode = "http://localhost:8000"
//counter node unique id (initialized on startup form arguments)
var nodeid string = ""

//item count map
//item unique id will be key and value will be count 
var m map[string]int =  make(map[string]int)

//lock for count map to avoid inconsistance
var lock = sync.RWMutex{}

//health check 
func CheckStatus(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(Status{Message : "Alive"})
}

//Get the count of specific item by id
func GetCount(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	json.NewEncoder(w).Encode(Message {ID : id, Count : read(id)})
}

//Sync the data of specific item by id(triggered by master node)
func SyncCount(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	count, err := strconv.Atoi(params["count"])
	if err == nil {
		write(id, read(id) + count) 
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
	fmt.Println("Sync complete for node : " + nodeid)
}

//Sync the data of all items(initialization of count map)
func SyncAll(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Syncing entire count")
	var messages map[string]int
	bodyBytes, er := ioutil.ReadAll(r.Body)
	if er != nil{
		fmt.Println(er)
	}
	fmt.Println("Body : "  + string(bodyBytes))
	
	err := json.Unmarshal(bodyBytes, &messages)
	if err == nil{
		m = messages
		w.WriteHeader(http.StatusOK)
		
	}else {
		fmt.Println(err)
		panic(err)
	}
}

//Update the count of unique item
func UpdateCount(w http.ResponseWriter, r *http.Request) {
	var message Message
	_ = json.NewDecoder(r.Body).Decode(&message)
	id := message.ID

	
	postMessage(id, message.Count)
	
	write(id, read(id) + message.Count) 

	json.NewEncoder(w).Encode(Message{ID : id, Count : read(id)})
}


func main() {

	router := mux.NewRouter()
	port := ""

	//start application with unique port number and unique node id 
	if len(os.Args) > 2 {
		_, err := strconv.Atoi(os.Args[1])
		if err != nil{
			panic("invalid port numner")
		} 
		port = os.Args[1]
		nodeid = os.Args[2]
	} else{
		panic("arguments missing : port, nodeid")
	}
	fmt.Println(port)
	fmt.Println(nodeid)

	//register the current counter node to master node (endpoint can be very based on the type of deployment)
	registerNode(nodeid, "http://localhost:" + port)
		
	router.HandleFunc("/items", UpdateCount).Methods("POST")
	router.HandleFunc("/items/{id}/count", GetCount).Methods("GET")
	router.HandleFunc("/sync/{id}/{count}", SyncCount).Methods("POST")
	router.HandleFunc("/syncall", SyncAll).Methods("POST")
	router.HandleFunc("/ping", CheckStatus).Methods("GET")
	log.Fatal(http.ListenAndServe(":"+ port, router))
}


//Util functions to read and write to map (to avoid inconsistant state)

func read(id string) int {
    lock.RLock()
    defer lock.RUnlock()
    return m[id]
}

func write(id string, count int) {
    lock.Lock()
    defer lock.Unlock()
    m[id] = count
}


//Master node communication function 
//Register current node to master node
func registerNode(id string, endpoint string){
	body := []byte("{\"id\" : \"" + id + "\", \"endpoint\" : \"" + endpoint + "\" }")
	rs, err := http.Post(masternode + "/addnode", "application/json",  bytes.NewBuffer(body))
	if rs == nil{
		panic("something went wrong")
	}
	if err != nil {
		panic(err)
	}
	//bodyBytes, err := ioutil.ReadAll(rs.Body)

	 if err != nil{
		 panic(err)
	 }
	 if rs.StatusCode != 200 {
		 panic("can not register node")
	 }
}
//Get message count form master node
//Not using it now. but can be used in case of in consistant state to sync by specific id
func getMessage(id string) int {
	 // Make a get request
	 rs, err := http.Get(masternode + "/items/" + id + "/count")
	 // Process response
	 if err != nil {
		 panic(err) // More idiomatic way would be to print the error and die unless it's a serious error
	 }
	 //defer rs.Body.Close()
  
	bodyBytes, err := ioutil.ReadAll(rs.Body)

	 if err != nil {
		 panic(err)
	 }
	 fmt.Println(string(bodyBytes))
	 var message Message
	_ = json.Unmarshal(bodyBytes, &message)
	fmt.Println(message.Count)
	return message.Count;
}

//Update the master node with current request 
func postMessage(id string, count int){
	
	body := []byte("{\"id\" : \"" + id + "\", \"count\" : " + strconv.Itoa(count) + ", \"sourceid\" : \"" + nodeid + "\" }")
	rs, err := http.Post(masternode + "/items", "application/json", bytes.NewBuffer(body))
	
	if rs == nil{
		panic("something went wrong")
	}
	if err != nil {
		panic(err)
	}
	
	if err != nil {
		panic(err)
	}
	//bodyBytes, err := ioutil.ReadAll(rs.Body)
	//  var message Message
	// _ = json.Unmarshal(bodyBytes, &message)
	// return message.Count
}