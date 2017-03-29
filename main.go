package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
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

	fmt.Println(rabbitAddress)

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
}

// END OF COPY PASTA FROM WORKER STUFF
// =================================================================================== //

var autoTxStore = make(map[string]llrb.LLRB)
var autoTxLookUp = make(map[types.AutoTxKey]types.AutoTxInit) // {stock, user} -> autoTx

var sampleATxCancel = types.AutoTxKey{
	Stock:  "AAPL",
	UserID: "Bob",
	Action: "Buy",
}

func insertTransaction(aTx types.AutoTxInit) {
	tree, found := autoTxStore[aTx.AutoTxKey.Stock]
	if !found {
		tree = *llrb.New()
	}
	tree.InsertNoReplace(aTx)
	autoTxLookUp[aTx.AutoTxKey] = aTx
	fmt.Printf("Inserting autoTx: %s\n", aTx.ToCSV())
	fmt.Println(tree)
	autoTxStore[aTx.AutoTxKey.Stock] = tree
	fmt.Println(autoTxStore)
}

func fillTransaction(item types.AutoTxInit) {

}

func cancelTransaction(aTxKey types.AutoTxKey) {
	tree, found := autoTxStore[aTxKey.Stock]
	if !found {
		// Tree doesn't exist. Throw err?
		fmt.Printf("Tree not found\n")
		return
	}
	autoTx, found := autoTxLookUp[aTxKey]
	if !found {
		// User has no autoTx. What a nerd.
		fmt.Printf("aTx not found\n")
		return
	}
	tree.Delete(autoTx) // Remove the transaction from the tree
	delete(autoTxLookUp, aTxKey)
	fmt.Println(tree)
}

func watchTriggers() {
	// chance param to something that makes sense. Like a quote obj?
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		fmt.Sprintf("%s:autoTx:updater", config.Redis.KeyPrefix), //name
		true,  // durable
		true,  // delete when unused
		false, // exclusive
		false, // no wait
		nil,   // arguments
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
		fmt.Printf("New Quote for %s at price %s\n", currQuote.Stock, currQuote.Price.String())
		tree, found := autoTxStore[currQuote.Stock] // tree, found
		if !found {
			// Tree doesn't exist. Throw err?
			fmt.Printf("Tree for stock %s does not exist\n", currQuote.Stock)
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
		tree.AscendGreaterOrEqual(modelATx, func(i llrb.Item) bool {
			autoTx := i.(types.AutoTxInit)
			fmt.Printf("Item has Trigger price %s, which is more than %f\n", autoTx.Trigger, currQuote.Price.ToFloat())
			// body := autoTx.ToCSV()
			// err = ch.Publish(
			// 	autoTxExchange,                // exchange
			// 	strconv.Itoa(autoTx.WorkerID), // routing key
			// 	false, // mandatory
			// 	false, // immediate
			// 	amqp.Publishing{
			// 		ContentType: "text/plain",
			// 		Headers: amqp.Table{
			// 			"transType": "autoTxInit",
			// 		},
			// 		Body: []byte(body),
			// 	})
			// failOnError(err, "Failed to publish a message")
			fmt.Println(tree)
			tree.Delete(i) //I have no idea if this is gonna shit the bed for multideletes
			fmt.Println(tree)
			// Remove from autoTxStore
			return true
		})

		//Buy
		tree.DescendLessOrEqual(modelATx, func(i llrb.Item) bool {
			autoTx := i.(types.AutoTxInit)
			fmt.Printf("Item has Trigger price %s, which is less than %f\n", autoTx.Trigger, currQuote.Price.ToFloat())
			// body := autoTx.ToCSV()
			// err = ch.Publish(
			// 	autoTxExchange,                // exchange
			// 	strconv.Itoa(autoTx.WorkerID), // routing key
			// 	false, // mandatory
			// 	false, // immediate
			// 	amqp.Publishing{
			// 		ContentType: "text/plain",
			// 		Headers: amqp.Table{
			// 			"transType": "autoTxInit",
			// 		},
			// 		Body: []byte(body),
			// 	})
			// failOnError(err, "Failed to publish a message")
			fmt.Println(tree)
			tree.Delete(i) //I have no idea if this is gonna shit the bed for multideletes
			fmt.Println(tree)
			// Remove from autoTxStore
			return true
		})

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
		fmt.Printf("Received a message: %s\n", d.Body)
		fmt.Printf("Message Type is: %s\n", d.Headers["transType"])

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
	fmt.Println("autoTx Manager Spinning")
	<-forever
}
