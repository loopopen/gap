// Package broker defines the v1 driver interfaces implemented by the bundled
// RabbitMQ and Kafka modules.
//
// Application code uses those modules' option functions. The interfaces stay
// compatible within v1 and change only in a release that updates the core
// module and every broker module together.
package broker
