package tinydb

func assert(b bool, message string) {
	if !b {
		panic(message)
	}
}
