package venmoscrape

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/urlfetch"
)

//Structure of the Venmo public API response
type Venmo struct {
	Paging struct {
		Next     string `json:"next"`
		Previous string `json:"previous"`
	} `json:"paging"`
	Data []struct {
		PaymentID   int    `json:"payment_id"`
		Permalink   string `json:"permalink"`
		Via         string `json:"via"`
		ActionLinks struct {
		} `json:"action_links"`
		StoryID     string        `json:"story_id"`
		Comments    []interface{} `json:"comments"`
		UpdatedTime time.Time     `json:"updated_time"`
		Audience    string        `json:"audience"`
		Actor       struct {
			Username   string `json:"username"`
			Picture    string `json:"picture"`
			About      string `json:"about"`
			Name       string `json:"name"`
			Firstname  string `json:"firstname"`
			NumFriends int    `json:"num_friends"`
			Lastname   string `json:"lastname"`
			Email      string `json:"email"`
			Phone      string `json:"phone"`
			Cancelled  bool   `json:"cancelled"`
			Friends    string `json:"friends"`
			ID         string `json:"id"`
		} `json:"actor"`
		Transactions []struct {
			Target struct {
				Username   string `json:"username"`
				Picture    string `json:"picture"`
				IsBusiness bool   `json:"is_business"`
				Name       string `json:"name"`
				Firstname  string `json:"firstname"`
				Lastname   string `json:"lastname"`
				Cancelled  bool   `json:"cancelled"`
				ExternalID string `json:"external_id"`
				ID         string `json:"id"`
			} `json:"target"`
		} `json:"transactions"`
		CreatedTime time.Time     `json:"created_time"`
		Mentions    []interface{} `json:"mentions"`
		Message     string        `json:"message"`
		Type        string        `json:"type"`
		Likes       struct {
			Count int           `json:"count"`
			Data  []interface{} `json:"data"`
		} `json:"likes"`
	} `json:"data"`
}

/* See:
http://matt.aimonetti.net/posts/2012/11/27/real-life-concurrency-in-go/
*/
func asyncHttpGets(c context.Context, urls []string) []*Venmo {
	ch := make(chan *Venmo)
	client := urlfetch.Client(c)
	responses := []*Venmo{}
	for _, url := range urls {
		go func(url string) {
			fmt.Printf("Fetching %s \n", url)
			res, httpError := client.Get(url)
			if httpError != nil {
				log.Fatal(httpError)
			}

			//parse response body and close IO
			data, err := ioutil.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				log.Fatal(err)
			}

			//venmo is struct of type Venmo
			venmo := &Venmo{}
			//Unmarshal JSON data to venmo struct, handle any errors
			e := json.Unmarshal(data, &venmo)
			if e != nil {
				panic(err)
			}
			//add the struct to the channel
			ch <- venmo
		}(url)
	}

	for {
		select {
		case r := <-ch:
			fmt.Printf("%s was fetched\n", r.Paging.Next)
			responses = append(responses, r)
			if len(responses) == len(urls) {
				return responses
			}
		case <-time.After(50 * time.Millisecond):
			fmt.Printf(".")
		}
	}
	return responses
}

// transactionKey returns the key used for all guestbook entries.
func transactionkKey(c context.Context) *datastore.Key {
	// The string "default_transaction" here could be varied to have multiple guestbooks.
	return datastore.NewKey(c, "Transaction", "default_transaction", 0, nil)
}

func init() {
	http.HandleFunc("/store", store)
}

type Pingback struct {
	EndingTime int64 `json:"endingtime"`
}

func makeURLs(startingTime int64, timeInterval int64, listSize int64) (int64, []string) {
	urls := make([]string, 0)
	const venmoURL string = "https://venmo.com/api/v5/public?until="

	var endingTime int64 = startingTime + (timeInterval * listSize)
	var currentTime int64 = startingTime

	for currentTime < endingTime {
		urls = append(urls, venmoURL+strconv.FormatInt(currentTime, 10))
		currentTime += timeInterval
	}
	return currentTime, urls

}

func store(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	urls := make([]string, 0)

	var fromString = r.URL.Query().Get("from")

	startingTime, err := strconv.ParseInt(fromString, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	currentTime, urls := makeURLs(startingTime, 5, 10)
	results := asyncHttpGets(c, urls)
	storeResults(w, r, c, results)
	pingback := Pingback{currentTime}
	js, err := json.Marshal(pingback)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func storeResults(w http.ResponseWriter, r *http.Request, c context.Context, results []*Venmo) {
	for _, result := range results {
		for _, element := range result.Data {
			key := datastore.NewIncompleteKey(c, "Transaction", transactionkKey(c))
			_, err := datastore.Put(c, key, &element)
			if err != nil {
				return
			}
		}
		http.Redirect(w, r, "/", http.StatusFound)
	}
}
