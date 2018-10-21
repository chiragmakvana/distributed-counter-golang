package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sync"
	"os"
	"strings"
	"fmt"
	"strconv"
	"io/ioutil"
	"bytes"

)

//item count message struct
type Message struct {
	ID        string	`json:"id"`
	Count 	  int  		`json:"count"`
	SourceId  string 	`json:"sourceid"`
}

//status message for ping request
type Status struct{
	Message string `json:"message"`
}
//Counter node struct to store the list of counter nodes
type CounterNode struct{
	ID string `json:"id"`
	EndPoint string `json:"endpoint"`
}

//item count map
//item unique id will be key and value will be count 
var m map[string]int =  make(map[string]int)
//lock for count map to avoid inconsistance
var lock = sync.RWMutex{}

//counter nodes map (endpoint will be key and node id will be value)
var counternodes map[string]string =  make(map[string]string)

//main function (starting point)
func main() {
	router := mux.NewRouter()
	
	//initial nodes can be added form the command line 
	if len(os.Args) > 1 {
		nodeEndpoints := strings.Split(os.Args[1], ",")
		for ind, node := range nodeEndpoints {
			counternodes[node] = strconv.Itoa(ind) 
		}
	} 


	fmt.Println("master node started on : 8000")
	router.HandleFunc("/items", UpdateCount).Methods("POST")
	router.HandleFunc("/items/{id}/count", GetCount).Methods("GET")
	router.HandleFunc("/ping", CheckStatus).Methods("GET")
	router.HandleFunc("/addnode", AddCounterNode).Methods("POST")
	router.HandleFunc("/removenode", RemoveCounterNode).Methods("POST")
	router.HandleFunc("/syncnode/{nodeid}", SyncCounterNode).Methods("POST")
	log.Fatal(http.ListenAndServe(":8000", router))
}


//---------------API Handlers---------------//
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

//update the local count data and broadcast the chanage to all other counter nodes
func UpdateCount(w http.ResponseWriter, r *http.Request) {

	var message Message
	_ = json.NewDecoder(r.Body).Decode(&message)
	id := message.ID
	
	write(id, read(id) + message.Count) 

	//broadcast in background routine
	go func(msg Message) {
        broadcast(msg)
	}(message)
	
	fmt.Println("Broadcast done")
	json.NewEncoder(w).Encode(Message{ID : id, Count : read(id)})
}

//Remove the counter node (in case of scale down)
func RemoveCounterNode(w http.ResponseWriter, r *http.Request) {
	//params := mux.Vars(r)
	var node CounterNode
	_ = json.NewDecoder(r.Body).Decode(&node)
	delete(counternodes, node.EndPoint)
	fmt.Println("node removed : " + node.EndPoint + " : " + node.ID)
	
	w.WriteHeader(http.StatusOK)
}

//add counter node (in case of scale up)
func AddCounterNode(w http.ResponseWriter, r *http.Request) {
	//params := mux.Vars(r)
	var node CounterNode
	_ = json.NewDecoder(r.Body).Decode(&node)
	var nodeExists = false
	for _, value := range counternodes {
		if value == node.ID {
			nodeExists = true
			break
		}
	}
	//Dont add the node if its already exist
	if nodeExists == false {
		counternodes[node.EndPoint] = node.ID
		
		fmt.Println("node added : " + node.EndPoint + " : " + node.ID)
		w.WriteHeader(http.StatusOK)
	} else {
		fmt.Println("node already exists : " + node.EndPoint + " : " + node.ID)

		w.WriteHeader(http.StatusBadRequest)
	}

	//json.NewEncoder(w).Encode(Message{ID : id, Count : read(id)})
}

//synce the counte node with all items data
func SyncCounterNode(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	nodeid := params["nodeid"]
	fmt.Println(nodeid)
	for key, value := range counternodes{
		fmt.Println(key, value)
		if value == nodeid{
			SyncNode(key)
			break
		}
	} 
	
	w.WriteHeader(http.StatusOK)
}


//---------------Util functions---------------//
//Util function to read to map (to avoid inconsistant state)
func read(id string) int {
    lock.RLock()
    defer lock.RUnlock()
    return m[id]
}
//Util function to write to map (to avoid inconsistant state)
func write(id string, count int) {
    lock.Lock()
    defer lock.Unlock()
    m[id] = count
}

//---------------Counter ndoe communication---------------//
//broadcast change to all counter node except the source node 
func broadcast (message Message) bool {

	//broadcast all other nodes for the latest change
	mapLength := len(counternodes)
	if mapLength > 1 {
		fmt.Println("Running for loop…")
		 
		for key, value := range counternodes {
			//update only if its not source node
			if message.SourceId != value {
				SyncMessge(key, message)
			}
		}
		fmt.Println("Finished for loop")
	}
	return true;
}

//sync the current change to specific counter node 
func SyncMessge(endpoint string, message Message) int{
	fmt.Println("Syncing " + endpoint)
	rs, err := http.Post(endpoint + "/sync/" + message.ID + "/" + strconv.Itoa(message.Count), "application/json", nil)
	if rs == nil{
		panic("something went wrong")
	}
	if err != nil {
		fmt.Println(err)
		 panic(err)
	}
	bodyBytes, err := ioutil.ReadAll(rs.Body)
	 
	 var responseMessage Message
	_ = json.Unmarshal(bodyBytes, &responseMessage)
	return message.Count
}

//synce the counter node with all items data (initialization of counter node with latest changes)
//TODO : need to check for the consistancy (as during this time it might update new changes)
func SyncNode(endpoint string){
	fmt.Println("Syncing node : " + endpoint)
	body, err := json.Marshal(m)
	if err == nil {
		rs, err := http.Post(endpoint + "/syncall", "application/json",  bytes.NewBuffer(body))
		if rs == nil{
			panic("something went wrong")
		}
		if err != nil {
			fmt.Println(err)
				panic(err)
		}
	}else{
		fmt.Println("Error", err)
	}
}