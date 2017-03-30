package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/distributeddesigns/currency"
	types "github.com/distributeddesigns/shared_types"
	logging "github.com/op/go-logging"
	"github.com/petar/GoLLRB/llrb"
	"github.com/streadway/amqp"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

// Holds values from <config>.yaml.
// 'PascalCase' values come from 'pascalcase' in x.yaml

// THIS NEEDS TO GET MOVED UP AND OUT OF BOTH THE WORKER AND THE TXMANS!!! @flagcommit
// =================================================================================== //

var (
	configFile = kingpin.
			Flag("config", "YAML file with service config").
			Default("./config/dev.yaml").
			Short('c').
			ExistingFile()
	rmqConn    *amqp.Connection
	consoleLog = logging.MustGetLogger("console")
	forever    = make(chan struct{})
)

const (
	autoTxQueue      = "autoTx"
	autoTxExchange   = "autoTx_resolved"
	quoteBroadcastEx = "quote_broadcast"
)

func failOnError(err error, msg string) {
	if err != nil {
		consoleLog.Fatalf("%s: %s", msg, err)
	}
}

var config struct {
	Rabbit struct {
		Host string
		Port int
		User string
		Pass string
	}

	Redis struct {
		Host        string
		Port        int
		MaxIdle     int    `yaml:"max idle connections"`
		MaxActive   int    `yaml:"max active connections"`
		IdleTimeout int    `yaml:"idle timeout"`
		KeyPrefix   string `yaml:"key prefix"`
	}

	QuotePolicy struct {
		BaseTTL    int `yaml:"base ttl"`
		BackoffTTL int `yaml:"backoff ttl"`
		MinTTL     int `yaml:"min ttl"`
	} `yaml:"quote policy"`
}

func loadConfig() {
	// Load the yaml file
	data, err := ioutil.ReadFile(*configFile)
	failOnError(err, "Could not read file")

	err = yaml.Unmarshal(data, &config)
	failOnError(err, "Could not unmarshal config")
}

func initRMQ() {
	rabbitAddress := fmt.Sprintf("amqp://%s:%s@%s:%d",
		config.Rabbit.User, config.Rabbit.Pass,
		config.Rabbit.Host, config.Rabbit.Port,
	)

	//fmt.Println(rabbitAddress)

	var err error
	rmqConn, err = amqp.Dial(rabbitAddress)
	failOnError(err, "Failed to rmqConnect to RabbitMQ")

	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	// closed in main()

	defer ch.Close()

	// Make sure all of the expected RabbitMQ exchanges and queues
	// exist before we start using them.
	// Recieve requests
	_, err = ch.QueueDeclare(
		autoTxQueue, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//RPC Init stuff (TODO)
	err = ch.ExchangeDeclare(
		autoTxExchange,      // name
		amqp.ExchangeDirect, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // args
	)
	failOnError(err, "Failed to declare exchange")
}

// END OF COPY PASTA FROM WORKER STUFF
// =================================================================================== //

// TreeKey : A key for accessing trees
type TreeKey struct {
	Stock, Action string
}

// AutoTxStore : The autoTxStorage struct
type AutoTxStore struct {
	AutoTxTrees  map[TreeKey]llrb.LLRB
	AutoTxLookup map[types.AutoTxKey]types.AutoTxInit
	Mutex        sync.RWMutex
}

// AutoTxStorage : The autoTxStorage struct
var AutoTxStorage = AutoTxStore{
	AutoTxTrees:  make(map[TreeKey]llrb.LLRB),
	AutoTxLookup: make(map[types.AutoTxKey]types.AutoTxInit), // {stock, user} -> autoTx
	Mutex:        sync.RWMutex{},
}

var sampleATxCancel = types.AutoTxKey{
	Stock:  "AAPL",
	UserID: "Bob",
	Action: "Buy",
}

func insertTransaction(aTx types.AutoTxInit) {
	currTreeKey := TreeKey{aTx.AutoTxKey.Stock, aTx.AutoTxKey.Action}
	AutoTxStorage.Mutex.Lock()
	tree, found := AutoTxStorage.AutoTxTrees[currTreeKey]
	if !found {
		tree = *llrb.New()
	}
	tree.InsertNoReplace(aTx)
	AutoTxStorage.AutoTxLookup[aTx.AutoTxKey] = aTx
	//fmt.Printf("Inserting autoTx: %s\n", aTx.ToCSV())
	//fmt.Println(tree)
	AutoTxStorage.AutoTxTrees[currTreeKey] = tree
	AutoTxStorage.Mutex.Unlock()
	//fmt.Println(autoTxStore)
}

func fillTransaction(item types.AutoTxInit) {

}

func cancelTransaction(aTxKey types.AutoTxKey) {
	currTreeKey := TreeKey{aTxKey.Stock, aTxKey.Action}
	AutoTxStorage.Mutex.Lock()
	tree, found := AutoTxStorage.AutoTxTrees[currTreeKey]
	AutoTxStorage.Mutex.Unlock()
	if !found {
		// Tree doesn't exist. Throw err?
		consoleLog.Debugf("Tree for stock %s with action %s not found\n", aTxKey.Stock, aTxKey.Action)
		return
	}
	AutoTxStorage.Mutex.Lock()
	autoTx, found := AutoTxStorage.AutoTxLookup[aTxKey]
	AutoTxStorage.Mutex.Unlock()
	if !found {
		// User has no autoTx. What a nerd.
		consoleLog.Debugf("aTx for stock %s with action %s not found\n", aTxKey.Stock, aTxKey.Action)
		return
	}
	AutoTxStorage.Mutex.Lock()
	tree.Delete(autoTx) // Remove the transaction from the tree
	AutoTxStorage.AutoTxTrees[currTreeKey] = tree
	delete(AutoTxStorage.AutoTxLookup, aTxKey)
	AutoTxStorage.Mutex.Unlock()
	//fmt.Println(tree)
}

func watchTriggers() {
	// chance param to something that makes sense. Like a quote obj?
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"autoTx:updater", //name
		true,             // durable
		true,             // delete when unused
		false,            // exclusive
		false,            // no wait
		nil,              // arguments
	)

	failOnError(err, "Failed to declare a receive queue")

	err = ch.QueueBind(
		q.Name,           //name
		"*.fresh",        // routing key
		quoteBroadcastEx, // exchange
		false,            // no-wait
		nil,              // args
	)

	failOnError(err, "Failed to bind to quotebroadcast queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to consume from quoteBroadcast Channel")

	for d := range msgs {
		currQuote, err := types.ParseQuote(string(d.Body[:]))
		failOnError(err, "Failed to parse Quote")
		consoleLog.Debugf("New Quote for %s at price %s\n", currQuote.Stock, currQuote.Price.String())
		AutoTxStorage.Mutex.Lock()
		buyTree, buyFound := AutoTxStorage.AutoTxTrees[TreeKey{currQuote.Stock, "Buy"}] // tree, found
		sellTree, sellFound := AutoTxStorage.AutoTxTrees[TreeKey{currQuote.Stock, "Sell"}]
		AutoTxStorage.Mutex.Unlock()
		if !buyFound && !sellFound {
			// Tree doesn't exist. Throw err?
			consoleLog.Debugf("BuyTree and SellTre for stock %s does not exist\n", currQuote.Stock)
			continue
		}
		// Get all trans less than or equal to trigger and fire them using the iterator in llrb (fillAutoTx)
		modelATx := types.AutoTxInit{
			AutoTxKey: types.AutoTxKey{
				Stock:  currQuote.Stock,
				UserID: "autoTxManager",
			},
			Amount:  currQuote.Price,
			Trigger: currQuote.Price,
		}

		//Sell
		sellTree.AscendGreaterOrEqual(modelATx, func(i llrb.Item) bool {
			autoTx := i.(types.AutoTxInit)
			fmt.Printf("Sell item has Trigger price %s, which is more than QuotePrice of %s\n", autoTx.Trigger, currQuote.Price)
			numStock, remCash := autoTx.Trigger.FitsInto(autoTx.Amount) // amount of stock we reserved from their port
			//fmt.Printf("Can fill %d stocks with remCash %f\n", numStock, remCash.ToFloat())
			filledPrice := currQuote.Price
			err = filledPrice.Mul(float64(numStock))
			filledPrice.Add(remCash) // Re-add the unfilled value
			failOnError(err, "Failed to multiply price")
			autoTxFilled := types.AutoTxFilled{
				AutoTxKey: autoTx.AutoTxKey,
				AddFunds:  filledPrice,
				AddStocks: 0,
			}
			body := autoTxFilled.ToCSV()
			err = ch.Publish(
				autoTxExchange,                // exchange
				strconv.Itoa(autoTx.WorkerID), // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: "text/csv",
					Headers: amqp.Table{
						"transType": "autoTxInit",
					},
					Body: []byte(body),
				})
			failOnError(err, "Failed to publish a message")
			//fmt.Println(sellTree)
			AutoTxStorage.Mutex.Lock()
			sellTree.Delete(i) //I have no idea if this is gonna shit the bed for multideletes
			AutoTxStorage.AutoTxTrees[TreeKey{currQuote.Stock, "Sell"}] = sellTree
			delete(AutoTxStorage.AutoTxLookup, autoTx.AutoTxKey)
			AutoTxStorage.Mutex.Unlock()
			//fmt.Println(sellTree)
			return true
		})
		AutoTxStorage.Mutex.Lock()
		AutoTxStorage.AutoTxTrees[TreeKey{currQuote.Stock, "Sell"}] = sellTree //update map with new sell tree TODO: POINTER STUFF SO THIS ISN'T SHIT
		AutoTxStorage.Mutex.Unlock()

		//Buy
		buyTree.DescendLessOrEqual(modelATx, func(i llrb.Item) bool {
			autoTx := i.(types.AutoTxInit)
			fmt.Printf("Buy item has Trigger price %s, which is less than QuotePrice of %s\n", autoTx.Trigger, currQuote.Price)

			filledStock, remCash := currQuote.Price.FitsInto(autoTx.Amount)
			autoTxFilled := types.AutoTxFilled{
				AutoTxKey: autoTx.AutoTxKey,
				AddFunds:  remCash,
				AddStocks: filledStock,
			}
			body := autoTxFilled.ToCSV()
			err = ch.Publish(
				autoTxExchange,                // exchange
				strconv.Itoa(autoTx.WorkerID), // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: "text/csv",
					Headers: amqp.Table{
						"transType": "autoTxInit",
					},
					Body: []byte(body),
				})
			failOnError(err, "Failed to publish a message")
			//fmt.Println(buyTree)
			AutoTxStorage.Mutex.Lock()
			buyTree.Delete(i) //I have no idea if this is gonna shit the bed for multideletes
			AutoTxStorage.AutoTxTrees[TreeKey{currQuote.Stock, "Buy"}] = buyTree
			delete(AutoTxStorage.AutoTxLookup, autoTx.AutoTxKey)
			AutoTxStorage.Mutex.Unlock()
			//fmt.Println(buyTree)
			// Remove from autoTxStore
			return true
		})
		AutoTxStorage.Mutex.Lock()
		AutoTxStorage.AutoTxTrees[TreeKey{currQuote.Stock, "Buy"}] = buyTree //update map with new buy tree TODO: POINTER STUFF SO THIS ISN'T SHIT
		AutoTxStorage.Mutex.Unlock()

		//fmt.Println(tree.Len())

		//DOES NOT IMPLEMENT BUY!!
	}
}

func processIncomingAutoTx() {
	// Take the autoTx out of Rabbit and start shoving them into the forest
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	msgs, err := ch.Consume(
		autoTxQueue, // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	failOnError(err, "Failed to consume from autoTxQueue")

	for d := range msgs {

		// get stuff from localquotecache and fill before it gets tree'd.
		//fmt.Printf("Received a message: %s\n", d.Body)
		//fmt.Printf("Message Type is: %s\n", d.Headers["transType"])

		if d.Headers["transType"] == "autoTxInit" {
			autoTx, err := types.ParseAutoTxInit(string(d.Body[:]))
			failOnError(err, "Failed to parse AutoTxInit")
			insertTransaction(autoTx)
		} else {
			autoTx, err := types.ParseAutoTxKey(string(d.Body[:]))
			failOnError(err, "Failed to parse AutoTxKey")
			cancelTransaction(autoTx)
		}
	}
}

func pushSampleATxInit() {
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	sampAmt, _ := currency.NewFromString(fmt.Sprintf("%d.%d", rand.Intn(100), rand.Intn(100)))
	sampTrig, _ := currency.NewFromString(fmt.Sprintf("%d.%d", rand.Intn(100), rand.Intn(100)))

	sampleATxInit := types.AutoTxInit{
		AutoTxKey: types.AutoTxKey{
			Stock:  "AAPL",
			UserID: "Bob",
			Action: "Buy",
		},
		Amount:   sampAmt,
		Trigger:  sampTrig,
		WorkerID: 4,
	}
	body := sampleATxInit.ToCSV()
	err = ch.Publish(
		"",          // exchange
		autoTxQueue, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Headers: amqp.Table{
				"transType": "autoTxInit",
			},
			Body: []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func pushSampleATxCancel() {
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	body := sampleATxCancel.ToCSV()
	err = ch.Publish(
		"",          // exchange
		autoTxQueue, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Headers: amqp.Table{
				"transType": "autoTxKey",
			},
			Body: []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	//Grab params and defaults
	kingpin.Parse()

	//Load configs
	loadConfig()

	//Dial RMQ
	initRMQ()
	defer rmqConn.Close()

	// Blocking read from RMQ
	go processIncomingAutoTx()
	go watchTriggers()
	// for i := 0; i < 5; i++ {
	// 	pushSampleATxInit()
	// }

	// pushSampleATxCancel()

	// On autoTx, doAutoTx
	//fmt.Println("autoTx Manager Spinning")
	<-forever
}
