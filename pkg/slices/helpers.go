package slices

func RemoveAt[V interface{}](s []V, i int) []V {
	return append(s[:i], s[i+1:]...)
}

func Reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// ContainsAll checks if s1 contains all s2 values in it.
// Note, even if there are duplications in s1 and s2 that will not be counted separately
func ContainsAll[T comparable](s1 []T, s2 ...T) bool {

	vm := make(map[T]struct{})
	for i := 0; i < len(s1); i++ {
		vm[s1[i]] = struct{}{}
	}
	beforeLength := len(vm)
	for i := 0; i < len(s2); i++ {
		vm[s2[i]] = struct{}{}
	}
	afterLength := len(vm)

	return beforeLength == afterLength
}

// ContainsAllByPointer checks if contains all for pointer tyeps
func ContainsAllByPointer[T comparable](s1 []*T, s2 ...*T) bool {
	vm := make(map[T]struct{})
	for i := 0; i < len(s1); i++ {
		if s1[i] == nil {
			continue
		}
		vm[*s1[i]] = struct{}{}
	}
	beforeLength := len(vm)
	for i := 0; i < len(s2); i++ {
		if s2[i] == nil {
			continue
		}
		vm[*s2[i]] = struct{}{}
	}
	afterLength := len(vm)

	return beforeLength == afterLength
}

func RemoveDuplicates[T comparable](s []T) []T {
	vm := make(map[T]struct{})
	for i := 0; i < len(s); i++ {
		vm[s[i]] = struct{}{}
	}
	result := make([]T, 0, len(vm))
	for k := range vm {
		result = append(result, k)
	}
	return result
}

func RetainByCondition[T any](s []*T, condition func(*T) bool) []*T {

	i := 0
	for _, v := range s {
		if condition(v) {
			s[i] = v
			i++
		}
	}
	return s[:i]
}
