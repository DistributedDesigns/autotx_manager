package main

import (
	"fmt"
	"io/ioutil"

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

}

// END OF COPY PASTA FROM WORKER STUFF
// =================================================================================== //

var autoTxStore = make(map[string]llrb.LLRB)

var sampAmt, _ = currency.NewFromString("150.55")
var sampTrig, _ = currency.NewFromString("50.00")

var sampleATxInit = types.AutoTxInit{
	Amount:   sampAmt,
	Trigger:  sampTrig,
	Stock:    "AAPL",
	UserID:   "Bob",
	WorkerID: 4,
}

var sampleATxCancel = types.AutoTxCancel{
	Stock:    "AAPL",
	UserID:   "Bob",
	WorkerID: 4,
}

func insertTransaction(aTx types.AutoTxInit) {
	tree, found := autoTxStore[aTx.Stock]
	if !found {
		tree = *llrb.New()
	}
	tree.InsertNoReplace(aTx)

	fmt.Printf("Inserting autoTx: %s\n", aTx.ToCSV())
	fmt.Println(tree)
}

func fillAndRemove(item types.AutoTxInit) {

}

func cancelTransaction(aTx types.AutoTxCancel) {
	_, found := autoTxStore[aTx.Stock]
	if !found {
		// Tree doesn't exist. Throw err?
		return
	}
	// tree.Delete(aTx) // Remove the transaction from the tree
}

func triggerIterator(item llrb.Item) {
	fillAndRemove(item.(types.AutoTxInit))
}

func watchTriggers() {
	// chance param to something that makes sense. Like a quote obj?
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	// CANT DIRECTLY CONSUME quoteBroadcastEx. Setup an exchange for it and then consume that. Get them fresh quotes. @flagcommit
	msgs, err := ch.Consume(
		quoteBroadcastEx, // queue
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)

	failOnError(err, "Failed to consume from quoteBroadcast Channel")

	go func() {
		for d := range msgs {
			currQuote, err := types.ParseQuote(string(d.Body[:]))
			failOnError(err, "Failed to parse Quote")
			fmt.Printf("New Quote for %s at price %s\n", currQuote.Stock, currQuote.Price.String())
			_, found := autoTxStore[currQuote.Stock] // tree, found
			if !found {
				// Tree doesn't exist. Throw err?
				return
			}
			// Get all trans less than or equal to trigger and fire them using the iterator in llrb (fillAutoTx)

			// tree.DescendLessOrEqual(currQuote, triggerIterator) // Shove via aTx object, or something else?
			// Also have to remove them
		}
	}()
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

	go func() {
		for d := range msgs {
			//Add to tree
			fmt.Printf("Received a message: %s\n", d.Body)
			fmt.Printf("Message Type is: %s\n", d.Headers["transType"])

			if d.Headers["transType"] == "autoTxInit" {
				autoTx, err := types.ParseAutoTxInit(string(d.Body[:]))
				failOnError(err, "Failed to parse AutoTxInit")
				insertTransaction(autoTx)
			} else {
				autoTx, err := types.ParseAutoTxCancel(string(d.Body[:]))
				failOnError(err, "Failed to parse AutoTxCancel")
				cancelTransaction(autoTx)
			}
		}
	}()
}

func pushSampleATxInit() {
	ch, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
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
				"transType": "autoTxCancel",
			},
			Body: []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func main() {
	//Grab params and defaults
	kingpin.Parse()

	//Load configs
	loadConfig()

	//Dial RMQ
	initRMQ()
	defer rmqConn.Close()

	// Blocking read from RMQ
	processIncomingAutoTx()
	watchTriggers()
	pushSampleATxInit()
	pushSampleATxCancel()

	// On autoTx, doAutoTx
	fmt.Println("autoTx Manager Spinning")
	<-forever
}
