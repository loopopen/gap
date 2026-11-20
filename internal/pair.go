package internal

type pair struct {
	Key   string
	Value string
}

// Pair returns a two-value tuple.
func Pair(key string, value string) pair {
	return pair{
		Key:   key,
		Value: value,
	}
}
