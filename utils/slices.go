package utils

func Pop[T any](slice []T) ([]T, *T) {
	if len(slice) == 0 {
		return slice, nil
	}
	return slice[:len(slice)-1], &slice[len(slice)-1]
}

func DeleteFromUnsorted[T comparable](slice []T, item T) []T {
	var index int
	for i, v := range slice {
		if v == item {
			index = i
			break
		}
	}

	slice[index] = slice[len(slice)-1]
	return slice[:len(slice)-1]
}
