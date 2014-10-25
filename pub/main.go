package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	mqtt "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	helper "github.com/cascades-fbp/cascades-mqtt/lib"
	"github.com/cascades-fbp/cascades/components/utils"
	"github.com/cascades-fbp/cascades/runtime"
	zmq "github.com/pebbe/zmq4"
)

var (
	// Flags
	inputEndpoint   = flag.String("port.in", "", "Component's input port endpoint")
	optionsEndpoint = flag.String("port.options", "", "Component's input port endpoint")
	errorEndpoint   = flag.String("port.err", "", "Component's output port endpoint")
	jsonFlag        = flag.Bool("json", false, "Print component documentation in JSON")
	debug           = flag.Bool("debug", false, "Enable debug mode")

	// Internal
	inPort, optionsPort, errPort *zmq.Socket
	err                          error
)

func validateArgs() {
	if *optionsEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *inputEndpoint == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func openPorts() {
	optionsPort, err = utils.CreateInputPort(*optionsEndpoint)
	utils.AssertError(err)

	inPort, err = utils.CreateInputPort(*inputEndpoint)
	utils.AssertError(err)

	if *errorEndpoint != "" {
		errPort, err = utils.CreateOutputPort(*errorEndpoint)
		utils.AssertError(err)
	}
}

func closePorts() {
	optionsPort.Close()
	inPort.Close()
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

	ch := utils.HandleInterruption()
	err = runtime.SetupShutdownByDisconnect(inPort, "mqtt-pub.in", ch)
	utils.AssertError(err)

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
	var customTopic string
	for {
		ip, err = inPort.RecvMessageBytes(0)
		if err != nil {
			log.Println("Error receiving message:", err.Error())
			continue
		}
		if !runtime.IsValidIP(ip) {
			continue
		}
		if runtime.IsOpenBracket(ip) {
			for {
				ip, err = inPort.RecvMessageBytes(0)
				if err != nil {
					log.Println("Error receiving message:", err.Error())
					continue
				}
				if !runtime.IsValidIP(ip) {
					continue
				}
				if runtime.IsCloseBracket(ip) {
					customTopic = ""
					break
				}
				if customTopic == "" {
					customTopic = string(ip[1])
					if customTopic == "" {
						customTopic = defaultTopic
					}
					continue
				}
				client.Publish(qos, customTopic, ip[1])
			}
			continue
		}
		client.Publish(qos, defaultTopic, ip[1])
	}
}
