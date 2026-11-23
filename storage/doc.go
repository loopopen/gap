// Package storage defines the v1 contract implemented by the bundled MySQL,
// PostgreSQL, and GORM modules.
//
// Application code uses those modules' option functions. It embeds [TxerBase]
// when it provides its own transaction. The [Storage] method set stays
// compatible within v1 and changes only in a release that updates the core
// module and every storage module together.
package storage
