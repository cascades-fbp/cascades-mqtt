package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	helper "github.com/cascades-fbp/cascades-mqtt/lib"
	"github.com/cascades-fbp/cascades/components/utils"
	"github.com/cascades-fbp/cascades/runtime"
	zmq "github.com/pebbe/zmq4"
)

var (
	// Flags
	optionsEndpoint = flag.String("port.options", "", "Component's input port endpoint")
	outputEndpoint  = flag.String("port.out", "", "Component's output port endpoint")
	errorEndpoint   = flag.String("port.err", "", "Component's output port endpoint")
	jsonFlag        = flag.Bool("json", false, "Print component documentation in JSON")
	debug           = flag.Bool("debug", false, "Enable debug mode")

	// Internal
	optionsPort, outPort, errPort *zmq.Socket
	err                           error
)

func validateArgs() {
	if *optionsEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *outputEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func openPorts() {
	optionsPort, err = utils.CreateInputPort(*optionsEndpoint)
	utils.AssertError(err)

	outPort, err = utils.CreateOutputPort(*outputEndpoint)
	utils.AssertError(err)

	if *errorEndpoint != "" {
		errPort, err = utils.CreateOutputPort(*errorEndpoint)
		utils.AssertError(err)
	}
}

func closePorts() {
	optionsPort.Close()
	outPort.Close()
	if errPort != nil {
		errPort.Close()
	}
	zmq.Term()
}

func main() {
	flag.Parse()

	if *jsonFlag {
		doc, _ := registryEntry.JSON()
		fmt.Println(string(doc))
		os.Exit(0)
	}

	log.SetFlags(0)
	if *debug {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	validateArgs()

	openPorts()
	defer closePorts()

	utils.HandleInterruption()

	log.Println("Waiting for options to arrive...")
	var (
		ip            [][]byte
		clientOptions *mqtt.ClientOptions
		client        *mqtt.MqttClient
		defaultTopic  string
		qos           mqtt.QoS
	)
	for {
		ip, err = optionsPort.RecvMessageBytes(0)
		if err != nil {
			log.Println("Error receiving IP:", err.Error())
			continue
		}
		if !runtime.IsValidIP(ip) || !runtime.IsPacket(ip) {
			continue
		}

		clientOptions, defaultTopic, qos, err = helper.ParseOptionsUri(string(ip[1]))
		if err != nil {
			log.Printf("Failed to parse connection uri. Error: %s", err.Error())
			continue
		}

		client = mqtt.NewClient(clientOptions)
		if _, err = client.Start(); err != nil {
			log.Printf("Failed to create MQTT client. Error: %s", err.Error())
			continue
		}

		defer client.Disconnect(1e6)

		optionsPort.Close()
		break
	}

	log.Println("Started...")
	topicFilter, err := mqtt.NewTopicFilter(defaultTopic, byte(qos))
	utils.AssertError(err)
	_, err = client.StartSubscription(messageHandler, topicFilter)
	utils.AssertError(err)

	ticker := time.Tick(1 * time.Second)
	for _ = range ticker {
	}
}

func messageHandler(client *mqtt.MqttClient, message mqtt.Message) {
	outPort.SendMessage(runtime.NewOpenBracket())
	outPort.SendMessage(runtime.NewPacket([]byte(message.Topic())))
	outPort.SendMessage(runtime.NewPacket(message.Payload()))
	outPort.SendMessage(runtime.NewCloseBracket())
}
