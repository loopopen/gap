// Package gap is the v1 application API.
//
// Symbols in this package, and the option functions of the broker and storage
// modules an application imports, stay compatible throughout v1. Message
// status semantics and the storage schema described in the README are part of
// that promise. A breaking change to this package requires a new major version.
//
// Packages under github.com/loopopen/gap/internal are not application API.
// Broker and storage modules from a different release are not a supported
// combination, because they share those packages.
package gap
