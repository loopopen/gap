package setx

type Set[T comparable] map[T]struct{}

// MakeSet creates a set containing the supplied values.
func MakeSet[T comparable](elems ...T) Set[T] {
	s := make(map[T]struct{}, len(elems))
	for _, e := range elems {
		s[e] = struct{}{}
	}
	return s
}

// Adds adds all supplied values to the set.
func (s Set[T]) Adds(elem T) {
	s[elem] = struct{}{}
}

// Has reports whether the set contains the supplied value.
func (s Set[T]) Has(elem T) bool {
	_, ok := s[elem]
	return ok
}

// func (s Set[T]) Removes(elem T) {
// 	delete(s, elem)
// }
